"""Bronze: landing CSVs -> raw Delta tables (append), typed by a _schema.json per folder."""
import gc
import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import polars as pl
from deltalake import DeltaTable

from .common import (TS, ParamsBase, RunResult, collect_streaming, compact, first_line,
                     new_run_id, param, utcnow, versions, write_options)
from .storage import tables_root


@dataclass
class BronzeParams(ParamsBase):
    WORKSPACE: str = param("MyWorkspace", "Fabric workspace name")
    LAKEHOUSE: str = param("MyLakehouse", "Lakehouse the feeds load into (bronze)")
    SCHEMA: str = param("dbo", "set to None if the Lakehouse is not schema-enabled")
    DRY_RUN: bool = param(False, "True = read and check every file, write nothing")
    STREAM_THRESHOLD_MB: int = param(500, "files this size or larger are streamed to Delta")
    LANDING_ROOT: str = param("landing", "parent folder under Files; each child folder is a feed")
    ARCHIVE_ROOT: str = param("archive", "loaded CSVs move to ARCHIVE_ROOT/<feed>")
    FOLDER: str = param("", '"" = discover every child of LANDING_ROOT; or one folder, e.g. "landing/orders" or "incoming/acme/orders"')
    TABLE: str = param("", "with FOLDER: target table name (overrides the schema file and the default)")
    TABLE_SUFFIX: str = param("_raw", "default table name = folder name + suffix")
    MISSING_SCHEMA: str = param("skip", 'folder without _schema.json: "skip" (flag it), "fail", or "text" (load every column as text)')
    FEED_FILTER: str = param("", 'with discovery: only these feeds, e.g. "customers" / "orders,products"')
    PARALLEL_FEEDS: int = param(1, "feeds loaded at once in this session (1 = one after another)")
    ROW_GROUP_ROWS: int = param(8_000_000, "max rows per Parquet row group; larger groups suit Direct Lake. None = library default (~1M)")
    LOG_TABLE: str = param("load_log", "Delta table that records every run (one row per file); None = don't log")


# --- Version check: which features this Polars has ---------------------------
HAS_SINK_DELTA = hasattr(pl.LazyFrame, "sink_delta")   # true streaming to Delta

# --- Schema files ---------------------------------------------------------------
TYPE_MAP = {
    "string": pl.Utf8, "utf8": pl.Utf8, "boolean": pl.Boolean, "bool": pl.Boolean,
    "int8": pl.Int8, "int16": pl.Int16, "int32": pl.Int32, "int64": pl.Int64,
    "uint8": pl.UInt8, "uint16": pl.UInt16, "uint32": pl.UInt32, "uint64": pl.UInt64,
    "float32": pl.Float32, "float64": pl.Float64, "date": pl.Date,
    "datetime": pl.Datetime("us", "UTC"),
}

DEFAULT_CSV_OPTIONS = {"separator": ",", "has_header": True}


def parse_type(name, where):
    key = name.strip().lower().replace(" ", "")
    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", key)
    if m:
        return pl.Decimal(int(m.group(1)), int(m.group(2)))
    if key in TYPE_MAP:
        return TYPE_MAP[key]
    raise ValueError(f"{where}: unknown type '{name}'")


def safe_name(folder):
    """Folder name -> table/feed name: lower case, letters, digits and underscores."""
    return re.sub(r"[^a-z0-9_]+", "_", folder.strip().lower()).strip("_")


def parse_schema(text, where):
    """A _schema.json file's text -> columns (Polars types), formats and CSV options."""
    spec = json.loads(text)
    if not spec.get("columns"):
        raise ValueError(f"{where}: 'columns' is missing or empty")
    columns = {c: parse_type(t, f"{where} column '{c}'") for c, t in spec["columns"].items()}
    formats = spec.get("formats", {})
    for c, t in columns.items():
        if (t == pl.Date or isinstance(t, pl.Datetime)) and c not in formats:
            raise ValueError(f"{where}: column '{c}' is {t} but has no entry in 'formats'")
    return {"table": spec.get("table"), "columns": columns, "formats": formats,
            "csv_options": spec.get("csv_options", dict(DEFAULT_CSV_OPTIONS))}


def typed_select(lf, cfg):
    """Cast text columns strictly to the schema's types, so a bad value fails the load
    instead of changing a column type."""
    exprs = []
    for c, dtype in cfg["columns"].items():
        e = pl.col(c).str.strip_chars()
        if dtype == pl.Date:
            e = e.str.to_date(cfg["formats"][c], strict=True)
        elif isinstance(dtype, pl.Datetime):
            e = e.str.to_datetime(cfg["formats"][c], time_unit="us", strict=True)
            if dtype.time_zone:
                e = e.dt.replace_time_zone(dtype.time_zone)
        elif dtype == pl.Boolean:
            e = e.str.to_lowercase().replace_strict(
                {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False},
                return_dtype=pl.Boolean)
        elif dtype != pl.Utf8:
            e = e.cast(dtype, strict=True)
        exprs.append(e.alias(c))
    return lf.select(exprs)


def add_audit(lf, file_name, load_ts):
    return lf.with_columns(
        pl.lit(file_name).alias("_source_file"),
        pl.lit(load_ts).cast(TS).alias("_load_ts"),
    )


SUMMARY_SCHEMA = {
    "feed": pl.Utf8, "folder": pl.Utf8, "target_table": pl.Utf8, "file": pl.Utf8,
    "size_mb": pl.Float64, "mode": pl.Utf8, "status": pl.Utf8, "rows": pl.Int64,
    "started_utc": TS, "finished_utc": TS, "seconds": pl.Float64, "error": pl.Utf8}


def result_row(feed, cfg, file="", size_mb=None, mode="", status="", rows=0,
               started=None, finished=None, error=None):
    return {"feed": feed, "folder": cfg["landing"], "target_table": cfg["table"], "file": file,
            "size_mb": None if size_mb is None else round(size_mb, 1), "mode": mode,
            "status": status, "rows": rows, "started_utc": started, "finished_utc": finished,
            "seconds": round((finished - started).total_seconds(), 1) if started and finished else 0.0,
            "error": error}


class BronzeRun:
    """One run of the feed loader."""

    def __init__(self, params, storage, log=print):
        self.p, self.storage, self.log = params, storage, log
        self.base = storage.lakehouse(params.WORKSPACE, params.LAKEHOUSE)
        self.write_opts = write_options(params.ROW_GROUP_ROWS)
        self.run_id = new_run_id()
        self.load_ts = utcnow()

    # --- paths ----------------------------------------------------------------
    def table_path(self, name):
        return f"{tables_root(self.base, self.p.SCHEMA)}/{name}"

    def files_path(self, rel):
        return f"{self.base}/Files/{rel}"

    # --- feed discovery ----------------------------------------------------------
    def read_schema_file(self, rel_folder):
        """Parsed _schema.json for a folder, or None if it has none."""
        try:
            text = self.storage.read_text(self.files_path(f"{rel_folder}/_schema.json"), 1_000_000)
        except Exception:
            return None
        return parse_schema(text, f"{rel_folder}/_schema.json")

    def build_feed(self, rel_folder, table_override=""):
        p = self.p
        name = safe_name(rel_folder.rstrip("/").rsplit("/", 1)[-1])
        spec = self.read_schema_file(rel_folder)
        if spec is None and p.MISSING_SCHEMA == "text":
            spec = {"table": None, "columns": None, "formats": {}, "csv_options": dict(DEFAULT_CSV_OPTIONS)}
        # archive mirrors the landing path: landing/orders -> archive/orders, incoming/acme/orders -> archive/incoming/acme/orders
        root = p.LANDING_ROOT
        rel_under_root = rel_folder[len(root) + 1:] if rel_folder.startswith(root + "/") else rel_folder
        feed = {"landing": rel_folder, "archive": f"{p.ARCHIVE_ROOT}/{rel_under_root}",
                "table": table_override or (spec or {}).get("table") or f"{name}{p.TABLE_SUFFIX}",
                "schema_source": "_schema.json" if spec and spec["columns"] else
                                 ("all text" if spec else "missing")}
        if spec:
            feed.update(columns=spec["columns"], formats=spec["formats"], csv_options=spec["csv_options"])
        return name, feed

    def subfolders(self, rel_folder):
        try:
            return sorted(f.name for f in self.storage.ls(self.files_path(rel_folder)) if f.is_dir)
        except Exception:
            return []

    def discover(self):
        """The feeds for this run, and result rows for folders skipped for having no schema file."""
        p = self.p
        if p.FOLDER:
            feeds = dict([self.build_feed(p.FOLDER.strip("/"), p.TABLE)])
        else:
            if p.TABLE:
                raise ValueError("TABLE can only be used together with FOLDER")
            feeds = dict(self.build_feed(f"{p.LANDING_ROOT}/{d}") for d in self.subfolders(p.LANDING_ROOT))
            if p.FEED_FILTER:
                wanted = {safe_name(f) for f in p.FEED_FILTER.split(",") if f.strip()}
                unknown = wanted - feeds.keys()
                if unknown:
                    raise ValueError(f"FEED_FILTER names feeds with no folder under {p.LANDING_ROOT}: {sorted(unknown)}")
                feeds = {k: v for k, v in feeds.items() if k in wanted}

        # two folders must never load into the same table
        tables = {}
        for name, f in feeds.items():
            if f["table"] in tables:
                raise ValueError(f"Folders '{tables[f['table']]}' and '{f['landing']}' both map to table {f['table']}")
            tables[f["table"]] = f["landing"]

        # folders without a schema file
        missing = {k: v for k, v in feeds.items() if v["schema_source"] == "missing"}
        if missing and p.MISSING_SCHEMA == "fail":
            raise ValueError("No _schema.json in: " + ", ".join(v["landing"] for v in missing.values()))
        skipped = [{"feed": k, "folder": v["landing"], "target_table": v["table"], "file": "", "size_mb": None,
                    "mode": "", "status": "no schema", "rows": 0, "started_utc": None, "finished_utc": None,
                    "seconds": 0.0, "error": f"no _schema.json in {v['landing']}; files left in place"}
                   for k, v in missing.items()]
        feeds = {k: v for k, v in feeds.items() if k not in missing}

        for k, v in feeds.items():
            self.log(f"{k:<20} {v['landing']:<35} -> {v['table']:<25} ({v['schema_source']})")
        for r in skipped:
            self.log(f"{r['feed']:<20} SKIPPED: {r['error']}")
        if not feeds:
            self.log("No feeds to load.")
        return feeds, skipped

    # --- loading -----------------------------------------------------------------
    def list_csvs(self, rel_folder):
        try:
            items = self.storage.ls(self.files_path(rel_folder))
        except Exception:
            return []
        # (path, size in MB), sorted by name
        return sorted((f.path, f.size / 1024**2) for f in items if f.name.lower().endswith(".csv"))

    def rows_for_file(self, target, file_name):
        """Rows in the table from this source file (0 if the table doesn't exist yet).
        Fast even on large tables: every row from one file has the same _source_file,
        so delta-rs skips other files' Parquet using their min/max stats."""
        if not DeltaTable.is_deltatable(target, storage_options=self.storage.options()):
            return 0
        return (pl.scan_delta(target, storage_options=self.storage.options())
                  .filter(pl.col("_source_file") == file_name)
                  .select(pl.len()).collect().item())

    def already_loaded(self, target, file_name):
        """Idempotency guard: has this source file already been appended?"""
        return self.rows_for_file(target, file_name) > 0

    def scan_feed_csv(self, path, cfg):
        """Lazy, typed read of one CSV. Everything is read as text, then cast strictly."""
        if cfg["columns"] is None:      # MISSING_SCHEMA = "text": the file's own header, every column as text
            return pl.scan_csv(path, infer_schema=False, storage_options=self.storage.options(),
                               null_values=["", "NULL", "null"], **cfg["csv_options"])
        lf = pl.scan_csv(path, schema={c: pl.Utf8 for c in cfg["columns"]},
                         storage_options=self.storage.options(),
                         null_values=["", "NULL", "null"], **cfg["csv_options"])
        return typed_select(lf, cfg)

    def load_feed(self, feed, cfg):
        """Load every new CSV for one feed, one file at a time.
        Returns one result row per file. A failure stops this feed only:
        the failed file stays in landing and later files wait for the next run."""
        target = self.table_path(cfg["table"])
        files = self.list_csvs(cfg["landing"])
        if not files:
            self.log(f"[{feed}] no new CSVs in {cfg['landing']}")
            now = utcnow()
            return [result_row(feed, cfg, status="no new files", started=now, finished=now)]
        out = []
        for src, size_mb in files:
            file_name = src.rsplit("/", 1)[-1]
            stream = size_mb >= self.p.STREAM_THRESHOLD_MB
            mode = ("stream" if HAS_SINK_DELTA else "stream-fallback") if stream else "memory"
            started = utcnow()
            try:
                status, rows = self.load_file(feed, cfg, target, src, file_name, size_mb, stream, mode)
            except Exception as e:
                msg = first_line(e)
                self.log(f"[{feed}] {file_name} FAILED: {msg}")
                out.append(result_row(feed, cfg, file_name, size_mb, mode, "failed", 0, started, utcnow(), msg))
                break
            out.append(result_row(feed, cfg, file_name, size_mb, mode, status, rows, started, utcnow()))
        return out

    def load_file(self, feed, cfg, target, src, file_name, size_mb, stream, mode):
        dry_run, opts = self.p.DRY_RUN, self.storage.options
        if self.already_loaded(target, file_name):
            self.log(f"[{feed}] {file_name} already loaded – archiving only")
            status, rows = "skipped", 0
        else:
            lf = add_audit(self.scan_feed_csv(src, cfg), file_name, self.load_ts)
            if dry_run:
                # null_count forces every cast to run, so bad values still fail here
                rows = collect_streaming(lf.select([pl.len().alias("_rows"), pl.all().null_count()]))["_rows"][0]
                status = "dry run"
            elif stream and HAS_SINK_DELTA:
                # never holds the whole file in memory; one Delta commit at the end
                lf.sink_delta(target, mode="append", storage_options=opts(), delta_write_options=self.write_opts)
                rows = self.rows_for_file(target, file_name)
                status = "loaded"
            else:
                # in memory; large files on older Polars also come here (streaming engine for the read)
                df = collect_streaming(lf) if stream else lf.collect()
                rows = df.height
                df.write_delta(target, mode="append", storage_options=opts(), delta_write_options=self.write_opts)
                del df                      # free this file before reading the next
                status = "loaded"
            del lf
            gc.collect()
            verb = "checked" if dry_run else "appended"
            self.log(f"[{feed}] {file_name} ({size_mb:,.0f} MB, {mode}): {verb} {rows:,} rows -> {cfg['table']}")
        if not dry_run:
            self.storage.move(src, self.files_path(f"{cfg['archive']}/{file_name}"))
        return status, rows

    # --- log -----------------------------------------------------------------------
    def write_log(self, summary, started, finished, workers):
        """Append this run's rows to LOG_TABLE. Returns an error message, or None."""
        p = self.p
        ctx = self.storage.context()
        pick = lambda *keys: next((str(ctx[k]) for k in keys if ctx.get(k) not in (None, "")), None)
        v = versions()
        log_df = summary.with_columns(
            pl.lit(self.run_id).alias("run_id"),
            pl.lit(started).cast(TS).alias("run_started_utc"),
            pl.lit(finished).cast(TS).alias("run_finished_utc"),
            pl.lit(p.WORKSPACE).alias("workspace"),
            pl.lit(p.LAKEHOUSE).alias("lakehouse"),
            pl.lit(pick("currentNotebookName"), dtype=pl.Utf8).alias("notebook_name"),
            pl.lit(str(ctx.get("isForPipeline", "")).lower() == "true").alias("triggered_by_pipeline"),
            pl.lit(pick("pipelineJobId", "pipelineRunId"), dtype=pl.Utf8).alias("pipeline_run_id"),
            pl.lit(pick("userName"), dtype=pl.Utf8).alias("user_name"),
            pl.lit(p.FOLDER or p.LANDING_ROOT).alias("source_root"),
            pl.lit(workers).cast(pl.Int32).alias("parallel_feeds"),
            pl.lit(v["polars"]).alias("polars_version"),
            pl.lit(v["deltalake"]).alias("deltalake_version"),
            pl.lit(json.dumps(ctx, default=str) if ctx else None, dtype=pl.Utf8).alias("context_json"),
        ).select(
            "run_id", "run_started_utc", "run_finished_utc", "workspace", "lakehouse",
            "feed", "folder", "target_table", "file", "size_mb", "mode", "status", "rows",
            "started_utc", "finished_utc", "seconds", "error",
            "notebook_name", "triggered_by_pipeline", "pipeline_run_id", "user_name",
            "source_root", "parallel_feeds", "polars_version", "deltalake_version", "context_json",
        )
        try:
            log_df.write_delta(self.table_path(p.LOG_TABLE), mode="append",
                               storage_options=self.storage.options(),
                               delta_write_options={"schema_mode": "merge"})
            self.log(f"Logged {log_df.height} row(s) to {p.LOG_TABLE} (run_id {self.run_id})")
            return None
        except Exception as e:
            err = first_line(e, 500)
            self.log(f"WARNING: could not write the load log: {err}")
            return err

    # --- run -------------------------------------------------------------------------
    def run(self):
        v = versions()
        self.log(f"polars {v['polars']}, deltalake {v['deltalake']} ({self.storage.kind} storage)")
        if not HAS_SINK_DELTA:
            self.log("NOTE: this Polars has no sink_delta, so large files are read whole into memory "
                     "instead of streamed. Pin a newer version for true streaming.")
        feeds, skipped = self.discover()

        # Run the feeds: one after another, or several at once on threads.
        # Polars and delta-rs release Python's GIL while they work, so threads run in parallel.
        started = utcnow()
        workers = max(1, min(self.p.PARALLEL_FEEDS, len(feeds) or 1))
        if workers == 1:
            per_feed = [self.load_feed(feed, cfg) for feed, cfg in feeds.items()]
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="feed") as pool:
                per_feed = list(pool.map(lambda kv: self.load_feed(*kv), feeds.items()))
        results = [r for feed_rows in per_feed for r in feed_rows] + skipped
        finished = utcnow()
        self.log(f"Run finished in {(finished - started).total_seconds():,.0f}s with {workers} feed(s) at a time")
        summary = pl.DataFrame(results, schema=SUMMARY_SCHEMA)

        # The log is written even when a feed failed, so failures are recorded too. Dry runs aren't logged.
        log_error = None
        if self.p.LOG_TABLE and not self.p.DRY_RUN:
            log_error = self.write_log(summary, started, finished, workers)
        elif self.p.DRY_RUN:
            self.log("Dry run: load log not written")

        problems = [f"{r['feed']}/{r['file']}: {r['error']}"
                    for r in summary.filter(pl.col("status") == "failed").iter_rows(named=True)]
        if log_error:
            problems.append(f"load log: {log_error}")
        exit_value = {"run_id": self.run_id,
                      "files": json.loads(summary.drop("started_utc", "finished_utc").write_json())}
        self.feeds = feeds
        return RunResult("bronze", self.run_id, summary, exit_value, problems)

    def maintain(self):
        """Compact and vacuum every feed's table. Appends add files every run."""
        feeds, _ = self.discover()
        for cfg in feeds.values():
            compact(self.table_path(cfg["table"]), self.storage, self.log)


def run(params, storage, log=print):
    """Load every new landing CSV. Returns a RunResult."""
    return BronzeRun(params, storage, log).run()


def maintain(params, storage, log=print):
    BronzeRun(params, storage, log).maintain()
