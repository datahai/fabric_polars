"""Gold: a dimensional model from one DuckDB SQL file per table (SCD1/SCD2 dimensions, facts)."""
import gc
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import duckdb
import polars as pl
from deltalake import DeltaTable

from .common import (TS, ParamsBase, RunResult, compact, first_line, merge_kwargs, new_run_id,
                     param, utcnow, versions, write_options)
from .storage import tables_root


@dataclass
class GoldParams(ParamsBase):
    SILVER_WORKSPACE: str = param("MyWorkspace", "Fabric workspace name")
    SILVER_LAKEHOUSE: str = param("ls_cleansed", "silver")
    SILVER_SCHEMA: str = param("dbo", "None if the Lakehouse is not schema-enabled")
    GOLD_WORKSPACE: str = param("", '"" = same as SILVER_WORKSPACE')
    GOLD_LAKEHOUSE: str = param("ls_gold", "gold")
    GOLD_SCHEMA: str = param("dbo", "None if the Lakehouse is not schema-enabled")
    SQL_FOLDER: str = param("config/gold", "under Files/ in the GOLD Lakehouse; one .sql file per table")
    TABLES: str = param("", '"" = every SQL file; or "dim_customer" / "dim_customer,fact_orders"')
    FULL_RELOAD: bool = param(False, "True: $watermark = 1900-01-01 (all rows); facts are overwritten. Dimensions keep their keys and history.")
    DRY_RUN: bool = param(False, "True: run the SQL and report what would change; write nothing")
    WATERMARK_LOOKBACK_MINUTES: int = param(60, "$watermark = last successful run start minus this overlap")
    ROW_GROUP_ROWS: int = param(8_000_000, "max rows per Parquet row group")
    LOG_TABLE: str = param("gold_load_log", "one row per table per run; $watermark comes from here")


EARLIEST = datetime(1900, 1, 1, tzinfo=timezone.utc)
LATEST = datetime(9999, 12, 31, tzinfo=timezone.utc)
SUCCESS = ("created", "merged", "appended", "overwritten", "reloaded", "no changes")

# --- SQL files -------------------------------------------------------------------------
SETTINGS = {"table", "type", "business_key", "surrogate_key", "scd", "history_columns",
            "unknown_member", "load", "depends_on", "description"}


def split_list(v):
    return [x.strip() for x in v.split(",") if x.strip()]


def parse_sql_file(name, text):
    """Settings from the leading `-- key: value` lines; the rest is the SQL."""
    settings, body, in_header = {}, [], True
    for line in text.splitlines():
        s = line.strip()
        m = re.match(r"^--\s*([A-Za-z_]+)\s*:\s*(.*?)\s*$", s) if in_header else None
        if m and m.group(1).lower() in SETTINGS:
            settings[m.group(1).lower()] = m.group(2)
            continue
        if in_header and (s == "" or s.startswith("--")):
            if m:   # looks like a setting but isn't one
                raise ValueError(f"{name}: unknown setting '{m.group(1)}' "
                                 f"(allowed: {', '.join(sorted(SETTINGS))})")
            continue
        in_header = False
        body.append(line)
    sql = "\n".join(body).strip().rstrip(";")
    where = name
    if not sql:
        raise ValueError(f"{where}: no SQL after the header")
    ttype = settings.get("type", "").lower()
    if ttype not in ("dimension", "fact"):
        raise ValueError(f"{where}: '-- type:' must be dimension or fact")
    table = settings.get("table") or re.sub(r"\.sql$", "", name, flags=re.I)
    keys = split_list(settings.get("business_key", ""))
    cfg = {"file": name, "table": table, "type": ttype, "sql": sql, "keys": keys,
           "depends_on": split_list(settings.get("depends_on", "")),
           "description": settings.get("description", "")}
    if ttype == "dimension":
        if not keys:
            raise ValueError(f"{where}: a dimension needs '-- business_key:'")
        sk = settings.get("surrogate_key", re.sub(r"^dim_", "", table) + "_sk")
        cfg["sk"] = None if sk.lower() == "none" else sk
        cfg["scd"] = int(settings.get("scd", "1"))
        if cfg["scd"] not in (1, 2):
            raise ValueError(f"{where}: '-- scd:' must be 1 or 2")
        if cfg["scd"] == 2 and not cfg["sk"]:
            raise ValueError(f"{where}: scd 2 needs a surrogate key (one business key has several versions)")
        cfg["history_columns"] = split_list(settings.get("history_columns", ""))
        cfg["unknown_member"] = settings.get("unknown_member", "true").lower() in ("true", "yes", "1")
        if "load" in settings:
            raise ValueError(f"{where}: 'load' is for facts; dimensions always merge")
    else:
        for s in ("surrogate_key", "scd", "history_columns", "unknown_member"):
            if s in settings:
                raise ValueError(f"{where}: '{s}' is for dimensions")
        load = settings.get("load", "merge" if keys else "overwrite").lower()
        if load not in ("merge", "append", "overwrite"):
            raise ValueError(f"{where}: '-- load:' must be merge, append or overwrite")
        if load == "merge" and not keys:
            raise ValueError(f"{where}: 'load: merge' needs '-- business_key:'")
        cfg["load"] = load
    # implicit dependencies: gold.<table> referenced in the SQL
    cfg["depends_on"] += [t for t in re.findall(r"\bgold\.([A-Za-z_][A-Za-z0-9_]*)", sql, flags=re.I)
                          if t != table and t not in cfg["depends_on"]]
    return cfg


def run_order(cfgs, selected):
    """Selected tables in dependency order. Dependencies outside the selection must already exist."""
    order, state = [], {}

    def visit(t, path):
        if state.get(t) == "done":
            return
        if state.get(t) == "visiting":
            raise ValueError("Dependency cycle: " + " -> ".join(path + [t]))
        state[t] = "visiting"
        for d in cfgs[t]["depends_on"]:
            if d not in cfgs:
                raise ValueError(f"{cfgs[t]['file']}: depends on '{d}', which has no SQL file")
            if d in selected:
                visit(d, path + [t])
        state[t] = "done"
        order.append(t)
    # dimensions first where nothing else decides the order
    for t in sorted(selected, key=lambda t: (cfgs[t]["type"] != "dimension", t)):
        visit(t, [])
    return order


def q(c):
    return '"' + c.replace('"', '""') + '"'


def check_keys(df, keys, cfg):
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"{cfg['file']}: business_key column(s) {missing} not in the SQL result")
    nulls = df.filter(pl.any_horizontal([pl.col(k).is_null() for k in keys])).height
    if nulls:
        raise ValueError(f"{cfg['file']}: {nulls:,} row(s) with a null business key")
    if df.select(pl.struct(keys).n_unique()).item() < df.height:
        sample = (df.group_by(keys).len().filter(pl.col("len") > 1).head(3)
                    .select(keys).to_dicts())
        raise ValueError(f"{cfg['file']}: business key not unique in the SQL result, e.g. {sample}")


def unknown_row(target_cols, schema, cfg):
    """The -1 'Unknown' member: key -1, text 'Unknown', everything else null; always current."""
    row = {}
    for c in target_cols:
        t = schema[c]
        if c == cfg["sk"]:
            row[c] = -1
        elif cfg["sk"] is None and c in cfg["keys"] and t.is_integer():
            row[c] = -1
        elif c == "valid_from":
            row[c] = EARLIEST
        elif c == "valid_to":
            row[c] = LATEST
        elif c == "is_current":
            row[c] = True
        elif c == "_gold_updated_utc":
            row[c] = utcnow()
        elif t == pl.Utf8:
            row[c] = "Unknown"
        else:
            row[c] = None
    return pl.DataFrame([row], schema=schema)


SUMMARY_SCHEMA = {
    "table": pl.Utf8, "type": pl.Utf8, "file": pl.Utf8, "status": pl.Utf8, "watermark": TS,
    "started_utc": TS, "error": pl.Utf8, "finished_utc": TS, "seconds": pl.Float64,
    "rows_in": pl.Int64, "inserted": pl.Int64, "updated": pl.Int64, "new_versions": pl.Int64,
    "expired": pl.Int64, "unchanged": pl.Int64}


class GoldRun:
    """One silver-to-gold run."""

    def __init__(self, params, storage, log=print):
        p = self.p = params
        self.storage, self.log = storage, log
        self.silver_base = storage.lakehouse(p.SILVER_WORKSPACE, p.SILVER_LAKEHOUSE)
        self.gold_base = storage.lakehouse(p.GOLD_WORKSPACE or p.SILVER_WORKSPACE, p.GOLD_LAKEHOUSE)
        self.write_opts = write_options(p.ROW_GROUP_ROWS)
        self.run_id = new_run_id()
        # DuckDB session: UTC, with schemas `silver` and `gold` for the SQL files
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone = 'UTC'")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # --- paths --------------------------------------------------------------------------
    def silver_path(self, name):
        return f"{tables_root(self.silver_base, self.p.SILVER_SCHEMA)}/{name}"

    def gold_path(self, name):
        return f"{tables_root(self.gold_base, self.p.GOLD_SCHEMA)}/{name}"

    def is_delta(self, path):
        return DeltaTable.is_deltatable(path, storage_options=self.storage.options())

    # --- SQL files ---------------------------------------------------------------------------
    def load_sql_files(self):
        p = self.p
        folder = f"{self.gold_base}/Files/{p.SQL_FOLDER}"
        try:
            items = self.storage.ls(folder)
        except Exception as e:
            raise FileNotFoundError(f"SQL folder not found: Files/{p.SQL_FOLDER} in {p.GOLD_LAKEHOUSE}") from e
        files = sorted(f.name for f in items if f.name.lower().endswith(".sql"))
        if not files:
            raise ValueError(f"No .sql files in Files/{p.SQL_FOLDER}")
        cfgs = {}
        for name in files:
            cfg = parse_sql_file(name, self.storage.read_text(f"{folder}/{name}", 10_000_000))
            if cfg["table"] in cfgs:
                raise ValueError(f"{name} and {cfgs[cfg['table']]['file']} both define {cfg['table']}")
            cfgs[cfg["table"]] = cfg
        return cfgs

    def plan(self):
        """Every table's settings, and the tables to run in dependency order."""
        cfgs = self.load_sql_files()
        if self.p.TABLES:
            wanted = split_list(self.p.TABLES)
            unknown = [t for t in wanted if t not in cfgs]
            if unknown:
                raise ValueError(f"TABLES names table(s) with no SQL file: {unknown}")
            selected = set(wanted)
        else:
            selected = set(cfgs)
        order = run_order(cfgs, selected)
        for t in order:
            c = cfgs[t]
            extra = (f"scd {c['scd']}, key {c['sk'] or c['keys']}" if c["type"] == "dimension" else f"load {c['load']}")
            deps = f" after {', '.join(c['depends_on'])}" if c["depends_on"] else ""
            self.log(f"{t:<22} {c['type']:<10} {extra}{deps}")
        return cfgs, order

    # --- sources, watermarks and writes -------------------------------------------------------
    def list_tables(self, base, schema):
        try:
            return sorted(f.name for f in self.storage.ls(tables_root(base, schema)) if f.is_dir)
        except Exception:
            return []

    def register(self, schema, name, path):
        """Expose a Delta table to DuckDB as <schema>.<name> (read lazily via Arrow; filters push down)."""
        ds = DeltaTable(path, storage_options=self.storage.options()).to_pyarrow_dataset()
        self.con.register(f"__{schema}_{name}", ds)
        self.con.execute(f'CREATE OR REPLACE VIEW {schema}."{name}" AS SELECT * FROM "__{schema}_{name}"')

    def register_sources(self):
        p = self.p
        for t in self.list_tables(self.silver_base, p.SILVER_SCHEMA):
            if self.is_delta(self.silver_path(t)):
                self.register("silver", t, self.silver_path(t))
        for t in self.list_tables(self.gold_base, p.GOLD_SCHEMA):
            if self.is_delta(self.gold_path(t)):
                self.register("gold", t, self.gold_path(t))
        self.log("silver: " + ", ".join(r[0] for r in self.con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver' ORDER BY 1").fetchall()))

    def watermark_for(self, table):
        """Start of this table's last successful run, minus the lookback; 1900-01-01 if none or FULL_RELOAD."""
        p = self.p
        if p.FULL_RELOAD:
            return EARLIEST
        path = self.gold_path(p.LOG_TABLE)
        if not self.is_delta(path):
            return EARLIEST
        last = (pl.scan_delta(path, storage_options=self.storage.options())
                  .filter((pl.col("table") == table) & pl.col("status").is_in(list(SUCCESS)))
                  .select(pl.col("run_started_utc").max()).collect().item())
        return (last - timedelta(minutes=p.WATERMARK_LOOKBACK_MINUTES)) if last else EARLIEST

    def run_sql(self, cfg, watermark):
        params = {"watermark": watermark} if "$watermark" in cfg["sql"] else None
        try:
            rel = self.con.execute(cfg["sql"], params) if params else self.con.execute(cfg["sql"])
            return rel.pl()
        except Exception as e:
            raise ValueError(f"{cfg['file']}: SQL failed: {str(e).splitlines()[0]}") from None

    def read_target(self, table):
        path = self.gold_path(table)
        return pl.read_delta(path, storage_options=self.storage.options()) if self.is_delta(path) else None

    def write(self, df, table, mode):
        opts = {**self.write_opts, "schema_mode": "overwrite"} if mode == "overwrite" else self.write_opts
        df.write_delta(self.gold_path(table), mode=mode, storage_options=self.storage.options(),
                       delta_write_options=opts)

    def merge(self, table, source, predicate, build):
        """delta-rs MERGE; `build` adds the when-clauses."""
        dt = DeltaTable(self.gold_path(table), storage_options=self.storage.options())
        return build(dt.merge(source.to_arrow(), **merge_kwargs(dt, predicate, self.write_opts))).execute()

    # --- dimensions --------------------------------------------------------------------------------
    def load_dimension(self, cfg, df, counts):
        dry_run = self.p.DRY_RUN
        table, keys, sk, scd = cfg["table"], cfg["keys"], cfg["sk"], cfg["scd"]
        check_keys(df, keys, cfg)
        reserved = [c for c in (sk, "valid_from", "valid_to", "is_current", "_gold_updated_utc") if c and c in df.columns]
        if reserved:
            raise ValueError(f"{cfg['file']}: the SQL must not return {reserved}; the notebook adds them")
        attrs = [c for c in df.columns if c not in keys]
        history = cfg["history_columns"] or attrs
        bad = [c for c in history if c not in attrs]
        if bad:
            raise ValueError(f"{cfg['file']}: history_columns {bad} are not attribute columns of the SQL result")
        now = utcnow()
        scd_cols = ["valid_from", "valid_to", "is_current"] if scd == 2 else []
        target_cols = ([sk] if sk else []) + keys + attrs + scd_cols + ["_gold_updated_utc"]
        df = df.sort(keys)                       # deterministic surrogate key order

        current = self.read_target(table)
        if current is None:
            # first build: keys 1..n, every member's first version covers all of history
            out = df
            if sk:
                out = out.with_row_index(sk, offset=1).with_columns(pl.col(sk).cast(pl.Int64))
            if scd == 2:
                out = out.with_columns(pl.lit(EARLIEST).cast(TS).alias("valid_from"),
                                       pl.lit(LATEST).cast(TS).alias("valid_to"),
                                       pl.lit(True).alias("is_current"))
            out = out.with_columns(pl.lit(now).cast(TS).alias("_gold_updated_utc")).select(target_cols)
            if cfg["unknown_member"]:
                out = pl.concat([unknown_row(target_cols, out.schema, cfg), out], how="vertical")
            counts["inserted"] = df.height
            if not dry_run:
                self.write(out, table, "overwrite")
            return "created"

        # existing table: same columns as the SQL result?
        have = list(current.columns)
        added = [c for c in target_cols if c not in have]
        removed = [c for c in have if c not in target_cols]
        if removed:
            raise ValueError(f"{cfg['file']}: gold.{table} has column(s) {removed} that the SQL no longer returns. "
                             f"Add them back, or drop the gold table to rebuild it (surrogate keys are then "
                             f"reassigned, so rebuild dependent facts too)")
        if added:
            # new attribute columns: add them (empty) keeping every key and version, then fill the
            # current rows in place this run, so a new column doesn't create a version for every member
            self.log(f"[{table}] adding column(s) {added}")
            current = current.with_columns([pl.lit(None).cast(df.schema[c]).alias(c) for c in added]).select(target_cols)
            if sk:   # the Unknown member gets 'Unknown' in new text columns, like at creation
                current = current.with_columns([pl.when(pl.col(sk) == -1).then(pl.lit("Unknown")).otherwise(pl.col(c)).alias(c)
                                                for c in added if df.schema[c] == pl.Utf8])
            if not dry_run:
                self.write(current, table, "overwrite")
            history = [c for c in history if c not in added]
            counts["columns_added"] = len(added)
        df = df.cast({c: current.schema[c] for c in df.columns})
        live = current.filter(pl.col("is_current")) if scd == 2 else current
        if sk:
            live = live.filter(pl.col(sk) != -1)
        # compare the SQL result with the current rows: existence by key, then column by column
        cur = live.select(([sk] if sk else []) + keys + attrs).rename({a: f"{a}__cur" for a in attrs})
        cmp = (df.join(cur, on=keys, how="left")
                 .join(live.select(keys).with_columns(pl.lit(True).alias("__exists")), on=keys, how="left")
                 .with_columns(pl.col("__exists").fill_null(False)))
        exists = pl.col("__exists")
        diff = lambda cols: pl.any_horizontal([~pl.col(c).eq_missing(pl.col(f"{c}__cur")) for c in cols] or [pl.lit(False)])
        t1_cols = [c for c in attrs if c not in history] if scd == 2 else attrs
        cmp = cmp.with_columns(
            (exists & diff(history)).alias("__new_version") if scd == 2 else pl.lit(False).alias("__new_version"),
        ).with_columns(
            (exists & ~pl.col("__new_version") & diff(t1_cols)).alias("__update"),
        )
        new = cmp.filter(~exists)
        versions_ = cmp.filter(pl.col("__new_version"))
        updates = cmp.filter(pl.col("__update"))
        counts["inserted"], counts["new_versions"] = new.height, versions_.height
        counts["expired"], counts["updated"] = versions_.height, updates.height
        counts["unchanged"] = cmp.height - new.height - versions_.height - updates.height

        missing_unknown = cfg["unknown_member"] and sk and current.filter(pl.col(sk) == -1).height == 0
        if new.height + versions_.height + updates.height == 0 and not missing_unknown:
            return "no changes"

        # new surrogate keys for new members and new versions
        next_sk = (current[sk].max() or 0) + 1 if sk else None
        inserts = pl.concat([new.with_columns(pl.lit(EARLIEST).cast(TS).alias("valid_from")),
                             versions_.with_columns(pl.lit(now).cast(TS).alias("valid_from"))], how="vertical")
        inserts = inserts.select(keys + attrs + ["valid_from"])
        if sk:
            inserts = inserts.with_row_index(sk, offset=next_sk).with_columns(pl.col(sk).cast(current.schema[sk]))
        inserts = inserts.with_columns(pl.lit(LATEST).cast(TS).alias("valid_to"), pl.lit(True).alias("is_current"),
                                       pl.lit(now).cast(TS).alias("_gold_updated_utc"))
        inserts = inserts.select(target_cols).with_columns(pl.lit("insert").alias("__action"))
        if missing_unknown:
            inserts = pl.concat([unknown_row(target_cols, current.schema, cfg).with_columns(pl.lit("insert").alias("__action")),
                                 inserts], how="vertical_relaxed")

        # rows that change an existing current row: expire (scd2 new version) or update in place
        def matched(frame, action):
            f = frame.select(keys + attrs).with_columns(pl.lit(now).cast(TS).alias("_gold_updated_utc"))
            if sk:
                f = f.with_columns(pl.lit(None).cast(current.schema[sk]).alias(sk))
            if scd == 2:
                f = f.with_columns(pl.lit(now).cast(TS).alias("valid_from"), pl.lit(None).cast(TS).alias("valid_to"),
                                   pl.lit(True).alias("is_current"))
            return f.select(target_cols).with_columns(pl.lit(action).alias("__action"))
        source = pl.concat([matched(versions_, "expire"), matched(updates, "update"), inserts], how="vertical_relaxed")
        # merge keys: set for expire/update rows, null for inserts (so they never match)
        source = source.with_columns([pl.when(pl.col("__action") != "insert").then(pl.col(k)).otherwise(None).alias(f"__mk_{k}")
                                      for k in keys]).with_columns(pl.col(c).cast(current.schema[c]) for c in target_cols)
        if dry_run:
            return "merged"
        pred = " AND ".join(f't.{q(k)} = s.{q("__mk_" + k)}' for k in keys)
        if scd == 2:
            pred += " AND t.is_current = true"

        def build(m):
            if scd == 2:
                m = m.when_matched_update(predicate="s.__action = 'expire'",
                                          updates={"valid_to": "s.valid_from", "is_current": "false",
                                                   "_gold_updated_utc": "s._gold_updated_utc"})
            upd = {c: f"s.{q(c)}" for c in t1_cols + ["_gold_updated_utc"]}
            m = m.when_matched_update(predicate="s.__action = 'update'", updates=upd)
            return m.when_not_matched_insert(updates={c: f"s.{q(c)}" for c in target_cols})
        self.merge(table, source, pred, build)
        return "merged"

    # --- facts -----------------------------------------------------------------------------------
    def load_fact(self, cfg, df, counts):
        p = self.p
        table, keys, load = cfg["table"], cfg["keys"], cfg["load"]
        sk_nulls = {c: n for c in df.columns if c.endswith("_sk") and (n := df[c].null_count())}
        if sk_nulls:
            raise ValueError(f"{cfg['file']}: null surrogate key(s) {sk_nulls}; use COALESCE(<key>, -1) for no match")
        if keys:
            check_keys(df, keys, cfg)
        if "_gold_updated_utc" in df.columns:
            raise ValueError(f"{cfg['file']}: the SQL must not return _gold_updated_utc; the notebook adds it")
        df = df.with_columns(pl.lit(utcnow()).cast(TS).alias("_gold_updated_utc"))
        exists = self.is_delta(self.gold_path(table))
        if not exists or p.FULL_RELOAD or load == "overwrite":
            counts["inserted"] = df.height
            if not p.DRY_RUN:
                self.write(df, table, "overwrite")
            return "created" if not exists else ("reloaded" if p.FULL_RELOAD else "overwritten")
        if load == "append":
            counts["inserted"] = df.height
            if not p.DRY_RUN:
                self.write(df, table, "append")
            return "appended"
        # merge on the business key; only rows whose values changed are updated
        target_cols = [f.name for f in DeltaTable(self.gold_path(table), storage_options=self.storage.options()).schema().fields]
        added = [c for c in df.columns if c not in target_cols]
        removed = [c for c in target_cols if c not in df.columns]
        if added or removed:
            raise ValueError(f"{cfg['file']}: SQL columns differ from gold.{table} (new: {added}, missing: {removed}); "
                             f"run with FULL_RELOAD = True to rebuild it")
        if p.DRY_RUN:
            existing = (pl.scan_delta(self.gold_path(table), storage_options=self.storage.options())
                          .select(keys).collect())
            hit = df.join(existing, on=keys, how="semi").height
            counts["inserted"], counts["updated"] = df.height - hit, hit   # updated = upper bound in a dry run
            return "merged"
        values = [c for c in df.columns if c not in keys and c != "_gold_updated_utc"]
        # each comparison in brackets: delta-rs mis-parses `a IS DISTINCT FROM b OR c ...` without them
        changed = " OR ".join(f"(t.{q(c)} IS DISTINCT FROM s.{q(c)})" for c in values) or "false"
        pred = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
        metrics = self.merge(table, df, pred,
                             lambda m: m.when_matched_update_all(predicate=changed).when_not_matched_insert_all())
        counts["inserted"] = metrics.get("num_target_rows_inserted", 0)
        counts["updated"] = metrics.get("num_target_rows_updated", 0)
        counts["unchanged"] = df.height - counts["inserted"] - counts["updated"]
        return "merged" if counts["inserted"] + counts["updated"] else "no changes"

    # --- log -----------------------------------------------------------------------------------------
    def write_log(self, summary, started, finished):
        """One row per table per run in LOG_TABLE. Returns an error message, or None."""
        p = self.p
        ctx = self.storage.context()
        v = versions()
        log = summary.with_columns(
            pl.lit(self.run_id).alias("run_id"),
            pl.lit(started).cast(TS).alias("run_started_utc"),
            pl.lit(finished).cast(TS).alias("run_finished_utc"),
            pl.lit(p.FULL_RELOAD).alias("full_reload"),
            pl.lit(p.SILVER_LAKEHOUSE).alias("silver_lakehouse"),
            pl.lit(p.GOLD_LAKEHOUSE).alias("gold_lakehouse"),
            pl.lit(str(ctx.get("currentNotebookName") or ""), dtype=pl.Utf8).alias("notebook_name"),
            pl.lit(str(ctx.get("isForPipeline", "")).lower() == "true").alias("triggered_by_pipeline"),
            pl.lit(v["polars"]).alias("polars_version"),
            pl.lit(v["duckdb"]).alias("duckdb_version"),
        )
        try:
            self.write(log, p.LOG_TABLE, "append")
            self.log(f"Logged run {self.run_id}")
            return None
        except Exception as e:
            err = first_line(e, 300)
            self.log(f"WARNING: could not write {p.LOG_TABLE}: {err}")
            return err

    # --- run ---------------------------------------------------------------------------------------------
    def run(self):
        """Tables run in dependency order. If a table fails, tables that depend on it are skipped."""
        p = self.p
        v = versions()
        self.log(f"polars {v['polars']}, deltalake {v['deltalake']}, duckdb {v['duckdb']} ({self.storage.kind} storage)")
        cfgs, order = self.plan()
        self.register_sources()
        started = utcnow()
        results, failed = [], set()
        for table in order:
            cfg = cfgs[table]
            counts = {"rows_in": 0, "inserted": 0, "updated": 0, "new_versions": 0, "expired": 0, "unchanged": 0}
            t0 = utcnow()
            row = {"table": table, "type": cfg["type"], "file": cfg["file"], "status": "", "watermark": None,
                   "started_utc": t0, "error": None}
            try:
                blocked = [d for d in cfg["depends_on"] if d in failed]
                if blocked:
                    raise RuntimeError(f"skipped: depends on {', '.join(blocked)}, which failed")
                wm = self.watermark_for(table)
                row["watermark"] = wm if "$watermark" in cfg["sql"] else None
                df = self.run_sql(cfg, wm)
                counts["rows_in"] = df.height
                if cfg["type"] == "dimension":
                    row["status"] = self.load_dimension(cfg, df, counts)
                else:
                    row["status"] = self.load_fact(cfg, df, counts)
                del df
                if p.DRY_RUN:
                    row["status"] = f"dry run ({row['status']})"
                elif self.is_delta(self.gold_path(table)):
                    self.register("gold", table, self.gold_path(table))     # later SQL sees the new data
                detail = (f"{counts['inserted']:,} inserted, {counts['updated']:,} updated"
                          + (f", {counts['new_versions']:,} new versions" if cfg["type"] == "dimension" and cfg["scd"] == 2 else "")
                          + f", {counts['unchanged']:,} unchanged")
                self.log(f"[{table}] {counts['rows_in']:,} rows from SQL -> {detail} ({row['status']})")
            except Exception as e:
                failed.add(table)
                msg = first_line(e)
                row["status"] = "skipped" if msg.startswith("skipped:") else "failed"
                msg = msg.removeprefix("skipped: ")
                row["error"] = msg
                self.log(f"[{table}] {row['status'].upper()}: {msg}")
            finally:
                row["finished_utc"] = utcnow()
                row["seconds"] = round((row["finished_utc"] - t0).total_seconds(), 1)
                results.append(row | counts)
                gc.collect()
        finished = utcnow()
        summary = pl.DataFrame(results, schema=SUMMARY_SCHEMA)
        self.log(f"Run finished in {(finished - started).total_seconds():,.0f}s")

        log_error = None
        if p.LOG_TABLE and not p.DRY_RUN:
            log_error = self.write_log(summary, started, finished)
        elif p.DRY_RUN:
            self.log("Dry run: nothing written")

        problems = [f"{r['table']}: {r['error']}"
                    for r in summary.filter(pl.col("status").is_in(["failed", "skipped"])).iter_rows(named=True)]
        if log_error:
            problems.append(f"log: {log_error}")
        exit_value = {"run_id": self.run_id,
                      "tables": json.loads(summary.drop("started_utc", "finished_utc", "watermark").write_json())}
        self.cfgs, self.order = cfgs, order
        return RunResult("gold", self.run_id, summary, exit_value, problems)

    def maintain(self):
        """Compact and vacuum the gold tables."""
        cfgs, order = self.plan()
        for t in order:
            compact(self.gold_path(t), self.storage, self.log)


def run(params, storage, log=print):
    """Build the gold tables from their SQL files. Returns a RunResult."""
    return GoldRun(params, storage, log).run()


def maintain(params, storage, log=print):
    GoldRun(params, storage, log).maintain()
