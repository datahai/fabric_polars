"""Silver: bronze tables -> cleansed tables, driven by a JSON rules file."""
import gc
import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import polars as pl
from deltalake import DeltaTable

from .common import (TS, ParamsBase, RunResult, collect_streaming, compact, first_line,
                     merge_kwargs, new_run_id, param, schema_of, utcnow, versions, write_options)
from .storage import tables_root


@dataclass
class SilverParams(ParamsBase):
    SOURCE_WORKSPACE: str = param("MyWorkspace", "Fabric workspace name")
    SOURCE_LAKEHOUSE: str = param("ls_raw", "bronze")
    SOURCE_SCHEMA: str = param("dbo", "None if the Lakehouse is not schema-enabled")
    TARGET_WORKSPACE: str = param("", '"" = same as SOURCE_WORKSPACE')
    TARGET_LAKEHOUSE: str = param("ls_cleansed", "silver")
    TARGET_SCHEMA: str = param("dbo", "None if the Lakehouse is not schema-enabled")
    RULES_FILE: str = param("config/silver_rules.json", "under Files/ in the TARGET Lakehouse")
    TABLES: str = param("", '"" = every table in the rules file; or "customers_raw" / "customers_raw,orders_raw"')
    FULL_RELOAD: bool = param(False, "True = rebuild the silver table(s) from all bronze rows")
    DRY_RUN: bool = param(False, "True = apply the rules and report counts, write nothing")
    PARALLEL_TABLES: int = param(1, "tables processed at once in this session")
    BATCH_ROWS: int = param(10_000_000, "new bronze files are processed in batches of about this many rows (a file is never split)")
    ROW_GROUP_ROWS: int = param(8_000_000, "max rows per Parquet row group")
    LOG_TABLE: str = param("silver_load_log", "one row per table per run")
    REJECTS_TABLE: str = param("silver_rejects", "one row per rejected value / row")
    PROCESSED_TABLE: str = param("silver_processed_files", "bronze source files already processed")


# --- Rules file --------------------------------------------------------------------
STRING_RULES = {"trim", "upper", "lower", "title_case", "collapse_spaces",
                "remove_control_chars", "null_if_empty", "replace", "map"}
NUMERIC_RULES = {"abs", "round"}
ANY_RULES = {"fill_null"}
ALLOWED_KEYS = {"values", "min", "max", "allow_null", "ignore_case", "on_fail", "default"}


def rule_name(rule):
    if isinstance(rule, str):
        return rule
    if isinstance(rule, dict) and len(rule) == 1:
        return next(iter(rule))
    raise ValueError(f"cleanse rule must be a name or a one-key object, got {rule!r}")


def parse_rules(text, rules_file):
    """Parse and check a rules file's text. Returns {bronze table: settings}."""
    spec = json.loads(text)
    tables = spec.get("tables")
    if not isinstance(tables, dict) or not tables:
        raise ValueError(f"{rules_file}: needs a non-empty 'tables' object")
    for src, t in tables.items():
        where = f"{rules_file} › {src}"
        unknown = set(t) - {"target_table", "duplicates", "cleanse", "allowed_values"}
        if unknown:
            raise ValueError(f"{where}: unknown setting(s) {sorted(unknown)}")
        d = t.get("duplicates")
        if d is not None:
            if not d.get("keys"):
                raise ValueError(f"{where}: duplicates needs 'keys'")
            if d.get("keep", "latest") not in ("latest", "first", "reject"):
                raise ValueError(f"{where}: duplicates.keep must be latest, first or reject")
        for col, rules in t.get("cleanse", {}).items():
            for r in rules:
                name = rule_name(r)
                if name not in STRING_RULES | NUMERIC_RULES | ANY_RULES:
                    raise ValueError(f"{where}: column '{col}': unknown cleanse rule '{name}'")
        for col, a in t.get("allowed_values", {}).items():
            bad = set(a) - ALLOWED_KEYS
            if bad:
                raise ValueError(f"{where}: allowed_values '{col}': unknown setting(s) {sorted(bad)}")
            on_fail = a.get("on_fail", "reject")
            if on_fail not in ("reject", "null", "default"):
                raise ValueError(f"{where}: allowed_values '{col}': on_fail must be reject, null or default")
            if on_fail == "default" and "default" not in a:
                raise ValueError(f"{where}: allowed_values '{col}': on_fail 'default' needs a 'default' value")
    return tables


def target_name(src, t):
    return t.get("target_table") or re.sub(r"_raw$", "", src)


def is_text(dtype):
    return dtype == pl.Utf8


def is_number(dtype):
    return dtype.is_numeric() or isinstance(dtype, pl.Decimal)


def to_value(v, dtype):
    """A JSON value as a Python value of the column's type (for min/max/values/defaults)."""
    if v is None:
        return None
    try:
        return _to_value(v, dtype)
    except Exception:
        hint = " (use YYYY-MM-DD)" if dtype == pl.Date else (
               " (use YYYY-MM-DD HH:MM:SS)" if isinstance(dtype, pl.Datetime) else "")
        raise ValueError(f"value {v!r} can't be used for a {dtype} column{hint}") from None


def _to_value(v, dtype):
    if dtype == pl.Date:
        return date.fromisoformat(str(v))
    if isinstance(dtype, pl.Datetime):
        d = datetime.fromisoformat(str(v))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    if isinstance(dtype, pl.Decimal):
        return Decimal(str(v))
    if dtype.is_integer():
        return int(v)
    if dtype.is_float():
        return float(v)
    if dtype == pl.Boolean:
        return bool(v)
    return str(v)


def lit(v, dtype):
    return pl.lit(to_value(v, dtype)).cast(dtype)


def cleanse_expr(col, rules, dtype, where):
    e = pl.col(col)
    for r in rules:
        name = rule_name(r)
        arg = r[name] if isinstance(r, dict) else None
        if name in STRING_RULES and not is_text(dtype):
            raise ValueError(f"{where}: '{name}' needs a text column, '{col}' is {dtype}")
        if name in NUMERIC_RULES and not is_number(dtype):
            raise ValueError(f"{where}: '{name}' needs a numeric column, '{col}' is {dtype}")
        if name == "trim":
            e = e.str.strip_chars()
        elif name == "upper":
            e = e.str.to_uppercase()
        elif name == "lower":
            e = e.str.to_lowercase()
        elif name == "title_case":
            e = e.str.to_titlecase()
        elif name == "collapse_spaces":
            e = e.str.replace_all(r"\s+", " ")
        elif name == "remove_control_chars":
            e = e.str.replace_all(r"\p{C}", "")
        elif name == "null_if_empty":
            e = pl.when(e.str.len_chars() == 0).then(None).otherwise(e)
        elif name == "replace":
            e = e.str.replace_all(arg["pattern"], arg.get("with", ""))
        elif name == "map":
            e = e.replace(arg)
        elif name == "abs":
            e = e.abs()
        elif name == "round":
            e = e.round(int(arg))
        elif name == "fill_null":
            try:
                e = e.fill_null(lit(arg, dtype))
            except ValueError as err:
                raise ValueError(f"{where}: column '{col}': fill_null {err}") from None
    return e.alias(col)


def allowed_fail_expr(col, a, dtype, where):
    """Boolean expression: True where the value breaks the column's allowed-values rule."""
    try:
        if a.get("on_fail") == "default":
            to_value(a["default"], dtype)
        return _allowed_fail_expr(col, a, dtype)
    except ValueError as e:
        raise ValueError(f"{where}: allowed_values '{col}': {e}") from None


def _allowed_fail_expr(col, a, dtype):
    c = pl.col(col)
    ok = pl.lit(True)
    if "values" in a:
        vals = [to_value(v, dtype) for v in a["values"]]
        if a.get("ignore_case") and is_text(dtype):
            ok = ok & c.str.to_lowercase().is_in([v.lower() for v in vals])
        else:
            ok = ok & c.is_in(vals)          # a plain list: no is_in ambiguity on any Polars version
    if "min" in a:
        ok = ok & (c >= lit(a["min"], dtype))
    if "max" in a:
        ok = ok & (c <= lit(a["max"], dtype))
    null_ok = a.get("allow_null", True)
    # a null passes (or fails) on allow_null alone; comparisons with null are null
    return pl.when(c.is_null()).then(pl.lit(not null_ok)).otherwise(~ok.fill_null(False))


def describe_rule(a):
    parts = []
    if "values" in a:
        parts.append(f"in {len(a['values'])} allowed values")
    if "min" in a:
        parts.append(f">= {a['min']}")
    if "max" in a:
        parts.append(f"<= {a['max']}")
    if not a.get("allow_null", True):
        parts.append("not null")
    return ", ".join(parts) or "allowed"


def prepare_rules(t, schema, src, where):
    """Check a table's rules against the bronze columns and types (before reading any rows),
    and build the Polars expressions."""
    dup = t.get("duplicates")
    keys = dup["keys"] if dup else []
    named = set(t.get("cleanse", {})) | set(t.get("allowed_values", {})) | set(keys)
    missing = sorted(c for c in named if c not in schema)
    if missing:
        raise ValueError(f"{where}: column(s) not in {src}: {missing}")
    return (
        [cleanse_expr(c, rules, schema[c], where) for c, rules in t.get("cleanse", {}).items()],
        {c: (a, allowed_fail_expr(c, a, schema[c], where)) for c, a in t.get("allowed_values", {}).items()},
        dup, keys)


# --- Batches and rejects --------------------------------------------------------------
def row_json(df):
    """Each row as a JSON string, every value as text."""
    return df.select(pl.struct(pl.all().cast(pl.Utf8)).struct.json_encode().alias("row_json"))


def reject_records(original, marked, column, rule, reason, src, tgt):
    """Rows for the rejects table. `marked` has _rn (row number in the batch) and
    optionally _value (the failing value); the original bronze row is joined back by _rn,
    so each value stays with its own row."""
    if marked.height == 0:
        return None
    if "_value" not in marked.columns:
        marked = marked.with_columns(pl.lit(None, dtype=pl.Utf8).alias("_value"))
    rows = marked.select("_rn", pl.col("_value").cast(pl.Utf8)).join(original, on="_rn", how="left")
    return pl.DataFrame({
        "source_table": [src] * rows.height,
        "target_table": [tgt] * rows.height,
        "source_file": rows["_source_file"].cast(pl.Utf8),
        "column": [column] * rows.height,
        "rule": [rule] * rows.height,
        "value": rows["_value"],
        "reason": [reason] * rows.height,
        "row_json": row_json(rows.drop("_rn", "_value"))["row_json"],
    })


def pending_files(lf):
    """New bronze files with their row counts, oldest load first."""
    return (lf.group_by("_source_file")
              .agg(pl.len().alias("rows"), pl.col("_load_ts").min().alias("loaded"))
              .sort(["loaded", "_source_file"])
              .collect())


def make_batches(files, batch_rows):
    """Group files (in load order) into batches of about batch_rows rows. A file is never split."""
    batches, cur, n = [], [], 0
    for f, rows in files.select("_source_file", "rows").iter_rows():
        if cur and n + rows > batch_rows:
            batches.append(cur)
            cur, n = [], 0
        cur.append(f)
        n += rows
    if cur:
        batches.append(cur)
    return batches


def apply_rules(df, prepared, src, tgt, counts, rejects, log=print):
    """Cleanse, check allowed values and remove duplicates for one batch. Returns the silver rows."""
    cleanse_exprs, allowed, dup, keys = prepared
    original = df
    # cleanse
    if cleanse_exprs:
        df = df.with_columns(cleanse_exprs)
    # allowed values
    reject_rn = []
    for col, (a, fail) in allowed.items():
        bad = df.filter(fail)
        if bad.height == 0:
            continue
        on_fail = a.get("on_fail", "reject")
        if on_fail == "reject":
            rejects.append(reject_records(original, bad.select("_rn", pl.col(col).alias("_value")), col,
                                          describe_rule(a), f"{col} not allowed ({describe_rule(a)})", src, tgt))
            reject_rn.append(bad.select("_rn"))
        else:
            new = pl.lit(None).cast(df.schema[col]) if on_fail == "null" else lit(a["default"], df.schema[col])
            df = df.with_columns(pl.when(fail).then(new).otherwise(pl.col(col)).alias(col))
            counts["values_replaced"] += bad.height
            log(f"[{src}] {bad.height:,} value(s) in {col} set to {on_fail}")
    if reject_rn:
        bad_rn = pl.concat(reject_rn).unique()
        df = df.join(bad_rn, on="_rn", how="anti")
        counts["rows_rejected"] += bad_rn.height
    # duplicates on the key columns
    if dup:
        null_key = df.filter(pl.any_horizontal([pl.col(k).is_null() for k in keys]))
        if null_key.height:
            rejects.append(reject_records(original, null_key.select("_rn"), ",".join(keys),
                                          "key not null", "null in key column(s)", src, tgt))
            df = df.join(null_key.select("_rn"), on="_rn", how="anti")
            counts["rows_rejected"] += null_key.height
        keep = dup.get("keep", "latest")
        # Cheap check first: if every key is unique there's nothing to resolve.
        # Otherwise find just the repeated keys and resolve only those rows.
        n_keys = (df[keys[0]].n_unique() if len(keys) == 1
                  else df.select(pl.struct(keys).n_unique()).item())
        if n_keys < df.height:
            dup_keys = collect_streaming(df.lazy().group_by(keys).len().filter(pl.col("len") > 1)).select(keys)
            dups = df.join(dup_keys, on=keys, how="semi")      # rows whose key repeats
            df = df.join(dup_keys, on=keys, how="anti")        # rows with a unique key
            if keep == "reject":
                rejects.append(reject_records(original, dups.select("_rn", pl.concat_str(
                    [pl.col(k).cast(pl.Utf8) for k in keys], separator="|").alias("_value")),
                    ",".join(keys), "unique key", "duplicate key in batch", src, tgt))
                counts["rows_rejected"] += dups.height
            else:
                winners = (dups.sort(["_load_ts", "_source_file", "_rn"])
                               .unique(subset=keys, keep="last" if keep == "latest" else "first",
                                       maintain_order=True))
                counts["duplicates_removed"] += dups.height - winners.height
                df = pl.concat([df, winners], how="vertical")
                del winners
            del dups, dup_keys
    del original
    # lineage: bronze _source_file/_load_ts are kept; _silver_updated_utc links to the run log
    return df.drop("_rn").with_columns(pl.lit(utcnow()).cast(TS).alias("_silver_updated_utc"))


SUMMARY_SCHEMA = {
    "source_table": pl.Utf8, "target_table": pl.Utf8, "status": pl.Utf8, "files": pl.Int64,
    "file_list": pl.Utf8, "batches": pl.Int64, "rows_read": pl.Int64, "rows_rejected": pl.Int64,
    "values_replaced": pl.Int64, "duplicates_removed": pl.Int64, "rows_out": pl.Int64,
    "rows_inserted": pl.Int64, "rows_updated": pl.Int64, "rows_appended": pl.Int64,
    "started_utc": TS, "finished_utc": TS, "seconds": pl.Float64, "error": pl.Utf8}


class SilverRun:
    """One bronze-to-silver run."""

    def __init__(self, params, storage, log=print):
        p = self.p = params
        self.storage, self.log = storage, log
        self.source_base = storage.lakehouse(p.SOURCE_WORKSPACE, p.SOURCE_LAKEHOUSE)
        self.target_base = storage.lakehouse(p.TARGET_WORKSPACE or p.SOURCE_WORKSPACE, p.TARGET_LAKEHOUSE)
        self.write_opts = write_options(p.ROW_GROUP_ROWS)
        self.run_id = new_run_id()
        self.write_errors = []

    # --- paths ----------------------------------------------------------------------
    def source_table_path(self, name):
        return f"{tables_root(self.source_base, self.p.SOURCE_SCHEMA)}/{name}"

    def target_table_path(self, name):
        return f"{tables_root(self.target_base, self.p.TARGET_SCHEMA)}/{name}"

    def is_delta(self, path):
        return DeltaTable.is_deltatable(path, storage_options=self.storage.options())

    # --- rules -----------------------------------------------------------------------
    def load_rules(self):
        p = self.p
        try:
            text = self.storage.read_text(f"{self.target_base}/Files/{p.RULES_FILE}", 10_000_000)
        except Exception as e:
            raise FileNotFoundError(f"Rules file not found: Files/{p.RULES_FILE} in {p.TARGET_LAKEHOUSE}") from e
        return parse_rules(text, p.RULES_FILE)

    def plan(self):
        """The tables for this run, and bronze tables with no rules (reported, not processed)."""
        p = self.p
        rules = self.load_rules()
        if p.TABLES:
            wanted = [t.strip() for t in p.TABLES.split(",") if t.strip()]
            unknown = [t for t in wanted if t not in rules]
            if unknown:
                raise ValueError(f"TABLES names table(s) with no entry in {p.RULES_FILE}: {unknown}")
            run_tables = {t: rules[t] for t in wanted}
        else:
            run_tables = dict(rules)
        # targets must be unique
        seen = {}
        for src, t in run_tables.items():
            tgt = target_name(src, t)
            if tgt in seen:
                raise ValueError(f"{seen[tgt]} and {src} both write to silver table {tgt}")
            seen[tgt] = src
        no_rules = []
        if not p.TABLES:
            try:
                bronze = sorted(f.name for f in self.storage.ls(tables_root(self.source_base, p.SOURCE_SCHEMA)) if f.is_dir)
            except Exception:
                bronze = []
            no_rules = [b for b in bronze if b not in rules and b != "load_log"]
        for src, t in run_tables.items():
            self.log(f"{src:<22} -> {target_name(src, t)}")
        for b in no_rules:
            self.log(f"{b:<22} not in {p.RULES_FILE}; skipped")
        return run_tables, no_rules

    # --- one table ---------------------------------------------------------------------
    def processed_files(self, src, tgt):
        """Bronze source files already processed from this bronze table into this silver table."""
        path = self.target_table_path(self.p.PROCESSED_TABLE)
        if not self.is_delta(path):
            return []
        return (pl.scan_delta(path, storage_options=self.storage.options())
                  .filter((pl.col("source_table") == src) & (pl.col("target_table") == tgt))
                  .select("source_file").unique().collect()
                  .to_series().to_list())

    def merge_into(self, target, source, keys, keep):
        """Upsert into the silver table on the key columns."""
        dt = DeltaTable(target, storage_options=self.storage.options())
        predicate = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
        m = dt.merge(source, **merge_kwargs(dt, predicate, self.write_opts))
        if keep == "first":
            m = m.when_not_matched_insert_all()                 # existing keys keep their first version
        else:
            m = m.when_matched_update_all().when_not_matched_insert_all()
        return m.execute()

    def process_table(self, src, t):
        p, opts = self.p, self.storage.options
        tgt = target_name(src, t)
        started = utcnow()
        counts = {"rows_read": 0, "rows_rejected": 0, "values_replaced": 0, "duplicates_removed": 0,
                  "rows_out": 0, "rows_inserted": 0, "rows_updated": 0, "rows_appended": 0}
        result = {"source_table": src, "target_table": tgt, "status": "", "files": 0, "file_list": None,
                  "batches": 0, "started_utc": started, "finished_utc": None, "seconds": 0.0, "error": None}
        rejects, processed, statuses = [], [], []
        try:
            bronze_path = self.source_table_path(src)
            if not self.is_delta(bronze_path):
                raise ValueError(f"bronze table {src} not found in {p.SOURCE_LAKEHOUSE}")
            where = f"{p.RULES_FILE} › {src}"

            # 1. check the rules against the bronze table's columns and types (before reading any rows)
            lf = pl.scan_delta(bronze_path, storage_options=opts())
            prepared = prepare_rules(t, schema_of(lf), src, where)
            dup, keys = prepared[2], prepared[3]

            # 2. new bronze files (all files on a full reload), oldest first, grouped into batches
            if not p.FULL_RELOAD:
                done = self.processed_files(src, tgt)
                if done:
                    lf = lf.filter(~pl.col("_source_file").is_in(done))
            files = pending_files(lf)
            result["files"] = files.height
            result["file_list"] = json.dumps(files["_source_file"].to_list())
            if files.height == 0:
                result["status"] = "no new data"
                return result | counts, rejects, processed
            batches = make_batches(files, p.BATCH_ROWS)
            result["batches"] = len(batches)
            target = self.target_table_path(tgt)

            # 3. each batch: rules, then one Delta commit
            for i, batch_files in enumerate(batches, 1):
                df = lf.filter(pl.col("_source_file").is_in(batch_files)).collect().with_row_index("_rn")
                counts["rows_read"] += df.height
                batch_rejects = []
                df = apply_rules(df, prepared, src, tgt, counts, batch_rejects, self.log)
                counts["rows_out"] += df.height
                gc.collect()
                first_write = (i == 1 and p.FULL_RELOAD) or not (p.DRY_RUN or self.is_delta(target))
                if p.DRY_RUN:
                    status = "dry run"
                elif first_write:
                    df.write_delta(target, mode="overwrite", storage_options=opts(),
                                   delta_write_options={**self.write_opts, "schema_mode": "overwrite"})
                    counts["rows_inserted"] += df.height
                    status = "reloaded" if p.FULL_RELOAD else "created"
                elif dup:
                    # hand delta-rs an Arrow copy and release the Polars frame first, so both aren't held
                    source = df.to_arrow()
                    df = None
                    gc.collect()
                    metrics = self.merge_into(target, source, keys, dup.get("keep", "latest"))
                    del source
                    counts["rows_inserted"] += metrics.get("num_target_rows_inserted", 0)
                    counts["rows_updated"] += metrics.get("num_target_rows_updated", 0)
                    status = "merged"
                else:
                    df.write_delta(target, mode="append", storage_options=opts(),
                                   delta_write_options=self.write_opts)
                    counts["rows_appended"] += df.height
                    status = "appended"
                statuses.append(status)
                # committed: these files (and their rejects) are done even if a later batch fails
                rejects += batch_rejects
                processed += [{"source_table": src, "target_table": tgt, "source_file": f} for f in batch_files]
                del df
                gc.collect()
                if len(batches) > 1:
                    self.log(f"[{src}] batch {i}/{len(batches)}: {len(batch_files)} file(s) done")

            result["status"] = statuses[0] if statuses[0] in ("dry run", "reloaded", "created") else statuses[-1]
            written = (f"{counts['rows_out']:,} rows would be written" if p.DRY_RUN else
                       f"{counts['rows_inserted']:,} inserted, {counts['rows_updated']:,} updated, "
                       f"{counts['rows_appended']:,} appended")
            self.log(f"[{src}] {counts['rows_read']:,} read, {counts['rows_rejected']:,} rejected, "
                     f"{counts['duplicates_removed']:,} duplicates removed -> {tgt}: {written} ({result['status']})")
        except Exception as e:
            result["status"] = "failed"
            result["error"] = first_line(e)
            self.log(f"[{src}] FAILED: {result['error']}"
                     + (f" ({len(processed)} file(s) in earlier batches were committed)" if processed else ""))
        finally:
            result["finished_utc"] = utcnow()
            result["seconds"] = round((result["finished_utc"] - started).total_seconds(), 1)
            gc.collect()
        # rejects and processed files of committed batches are kept, even if a later batch failed
        return result | counts, [r for r in rejects if r is not None], processed

    # --- records ---------------------------------------------------------------------------
    def append(self, table, df):
        if df is None or df.height == 0:
            return
        try:
            df.write_delta(self.target_table_path(table), mode="append",
                           storage_options=self.storage.options(), delta_write_options={"schema_mode": "merge"})
        except Exception as e:
            self.write_errors.append(f"{table}: {first_line(e, 300)}")
            self.log(f"WARNING: could not write {table}: {self.write_errors[-1]}")

    def write_records(self, outcomes, summary, started, finished):
        """Rejects, processed files and the run log: written after all tables finish, one commit each.
        Nothing from a failed table is marked as processed, so its files are picked up again next run."""
        p = self.p
        now = pl.lit(utcnow()).cast(TS)
        if p.DRY_RUN:
            n = sum(r.height for o in outcomes for r in o[1])
            self.log(f"Dry run: nothing written ({n:,} reject record(s) would be written)")
            return
        rej = [r for o in outcomes for r in o[1]]
        if rej:
            self.append(p.REJECTS_TABLE, pl.concat(rej, how="vertical_relaxed").with_columns(
                pl.lit(self.run_id).alias("run_id"), now.alias("rejected_utc")))
        done = [x for o in outcomes for x in o[2]]
        if done:
            self.append(p.PROCESSED_TABLE, pl.DataFrame(done, schema={
                "source_table": pl.Utf8, "target_table": pl.Utf8, "source_file": pl.Utf8})
                .with_columns(pl.lit(self.run_id).alias("run_id"), now.alias("processed_utc")))
        if p.LOG_TABLE:
            ctx = self.storage.context()
            v = versions()
            self.append(p.LOG_TABLE, summary.with_columns(
                pl.lit(self.run_id).alias("run_id"),
                pl.lit(started).cast(TS).alias("run_started_utc"),
                pl.lit(finished).cast(TS).alias("run_finished_utc"),
                pl.lit(p.FULL_RELOAD).alias("full_reload"),
                pl.lit(p.SOURCE_LAKEHOUSE).alias("source_lakehouse"),
                pl.lit(p.TARGET_LAKEHOUSE).alias("target_lakehouse"),
                pl.lit(p.RULES_FILE).alias("rules_file"),
                pl.lit(str(ctx.get("currentNotebookName") or ""), dtype=pl.Utf8).alias("notebook_name"),
                pl.lit(str(ctx.get("isForPipeline", "")).lower() == "true").alias("triggered_by_pipeline"),
                pl.lit(v["polars"]).alias("polars_version"),
                pl.lit(v["deltalake"]).alias("deltalake_version"),
            ))
        self.log(f"Recorded run {self.run_id}")

    # --- run ---------------------------------------------------------------------------------
    def run(self):
        v = versions()
        self.log(f"polars {v['polars']}, deltalake {v['deltalake']} ({self.storage.kind} storage)")
        run_tables, no_rules = self.plan()
        started = utcnow()
        workers = max(1, min(self.p.PARALLEL_TABLES, len(run_tables) or 1))
        if workers == 1:
            outcomes = [self.process_table(s, t) for s, t in run_tables.items()]
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="table") as pool:
                outcomes = list(pool.map(lambda kv: self.process_table(*kv), run_tables.items()))
        finished = utcnow()

        results = [o[0] for o in outcomes]
        for b in no_rules:
            results.append({"source_table": b, "target_table": None, "status": "no rules", "files": 0,
                            "file_list": None, "batches": 0, "rows_read": 0, "rows_rejected": 0,
                            "values_replaced": 0, "duplicates_removed": 0, "rows_out": 0, "rows_inserted": 0,
                            "rows_updated": 0, "rows_appended": 0, "started_utc": None, "finished_utc": None,
                            "seconds": 0.0, "error": None})
        summary = pl.DataFrame(results, schema=SUMMARY_SCHEMA)
        self.log(f"Run finished in {(finished - started).total_seconds():,.0f}s with {workers} table(s) at a time")
        self.write_records(outcomes, summary, started, finished)

        problems = [f"{r['source_table']}: {r['error']}"
                    for r in summary.filter(pl.col("status") == "failed").iter_rows(named=True)]
        problems += self.write_errors
        exit_value = {"run_id": self.run_id,
                      "tables": json.loads(summary.drop("started_utc", "finished_utc", "file_list").write_json())}
        self.run_tables = run_tables
        return RunResult("silver", self.run_id, summary, exit_value, problems)

    def maintain(self):
        """Compact and vacuum the silver tables. Merges rewrite files."""
        run_tables, _ = self.plan()
        for src, t in run_tables.items():
            compact(self.target_table_path(target_name(src, t)), self.storage, self.log)


def run(params, storage, log=print):
    """Cleanse new bronze rows into silver. Returns a RunResult."""
    return SilverRun(params, storage, log).run()


def maintain(params, storage, log=print):
    SilverRun(params, storage, log).maintain()
