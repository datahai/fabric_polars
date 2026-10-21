"""Helpers shared by the bronze, silver and gold layers."""
import inspect
import json
import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone

import polars as pl
import deltalake
from deltalake import DeltaTable

TS = pl.Datetime("us", "UTC")


def utcnow():
    return datetime.now(timezone.utc)


def new_run_id():
    return str(uuid.uuid4())


def first_line(err, limit=1000):
    """The first line of an exception message, shortened for logs and summaries."""
    return str(err).splitlines()[0][:limit] if str(err) else type(err).__name__


def collect_streaming(lf):
    """Run a lazy query with the streaming engine on any Polars version."""
    try:
        return lf.collect(engine="streaming")     # Polars 1.2x and later
    except (TypeError, ValueError):
        return lf.collect(streaming=True)         # older Polars


def schema_of(lf):
    """A LazyFrame's column names and types, on any Polars version."""
    try:
        return dict(lf.collect_schema())
    except AttributeError:
        return dict(lf.schema)                    # older Polars


def write_options(row_group_rows):
    """delta-rs write options: new source columns allowed, large row groups for Direct Lake."""
    opts = {"schema_mode": "merge"}
    if row_group_rows:
        from deltalake import WriterProperties
        opts["writer_properties"] = WriterProperties(max_row_group_size=row_group_rows)
    return opts


def merge_kwargs(dt, predicate, write_opts):
    """Arguments for DeltaTable.merge: streamed execution where available (less memory)."""
    params = inspect.signature(dt.merge).parameters
    kwargs = {"predicate": predicate, "source_alias": "s", "target_alias": "t"}
    if "streamed_exec" in params:
        kwargs["streamed_exec"] = True
    if "writer_properties" in params and "writer_properties" in write_opts:
        kwargs["writer_properties"] = write_opts["writer_properties"]
    return kwargs


def compact(path, storage, log=print, retention_hours=168):
    """Compact small files and vacuum old ones in one Delta table (if it exists)."""
    if not DeltaTable.is_deltatable(path, storage_options=storage.options()):
        return
    dt = DeltaTable(path, storage_options=storage.options())
    log(f"{path.rsplit('/', 1)[-1]}: {dt.optimize.compact()}")
    dt.vacuum(retention_hours=retention_hours, dry_run=False, enforce_retention_duration=True)


# --- Parameters ---------------------------------------------------------------
def param(default, help=""):
    """A run parameter: its default, and the comment shown in the notebook's parameter cell."""
    return field(default=default, metadata={"help": help})


class ParamsBase:
    """Base for each layer's parameters. Field names match the notebook's parameter cell."""

    @classmethod
    def from_mapping(cls, values):
        """Build from a dict (or a notebook's globals()), taking only this layer's parameters."""
        names = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in values.items() if k in names})

    def updated(self, **changes):
        """A copy with some parameters changed. Unknown names are an error."""
        names = {f.name for f in fields(self)}
        unknown = sorted(set(changes) - names)
        if unknown:
            raise ValueError(f"unknown parameter(s) for {type(self).__name__}: {unknown}")
        values = {f.name: getattr(self, f.name) for f in fields(self)}
        values.update(changes)
        return type(self)(**values)


# --- Results ------------------------------------------------------------------
@dataclass
class RunResult:
    """What a layer run returns: the summary table, the notebook exit value, and any problems."""
    layer: str
    run_id: str
    summary: pl.DataFrame
    exit_value: dict
    problems: list = field(default_factory=list)

    @property
    def ok(self):
        return not self.problems

    def exit_json(self):
        return json.dumps(self.exit_value)

    def check(self):
        """Raise if anything failed, so a pipeline (or the CLI) marks the run failed."""
        if self.problems:
            raise RuntimeError(f"{len(self.problems)} problem(s): " + "; ".join(self.problems))


def versions():
    out = {"polars": pl.__version__, "deltalake": deltalake.__version__}
    try:
        import duckdb
        out["duckdb"] = duckdb.__version__
    except ImportError:
        pass
    return out
