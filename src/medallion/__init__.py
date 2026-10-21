"""Medallion pipeline engine: bronze (CSV -> raw Delta), silver (rules), gold (dimensional SQL).

The same code runs in Fabric notebooks (copied into them by scripts/build_notebooks.py) and
on a PC through the `medallion` command, against local folders (OneLake from a PC is next).

    from medallion import LocalStorage, bronze
    result = bronze.run(bronze.BronzeParams(LAKEHOUSE="ls_raw"), LocalStorage("C:/lakehouses"))
"""
__version__ = "0.1.0"
__all__ = ["FabricStorage", "LocalStorage", "RunResult", "__version__"]


def __getattr__(name):
    # imported on first use, so `medallion.deploy` works without Polars installed
    if name in ("FabricStorage", "LocalStorage"):
        from . import storage
        return getattr(storage, name)
    if name == "RunResult":
        from .common import RunResult
        return RunResult
    raise AttributeError(f"module 'medallion' has no attribute {name!r}")
