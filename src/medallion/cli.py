"""The `medallion` command: run the pipeline on this PC against local Lakehouse folders.

    medallion init C:\\lakehouses --data sample     # create Lakehouse folders, copy config and test data
    medallion run all --root C:\\lakehouses          # bronze, then silver, then gold
    medallion run silver --dry-run --tables customers_raw
    medallion run gold --set FULL_RELOAD=true
    medallion params gold                           # every parameter, its default and meaning
    medallion log bronze -n 20                      # recent runs from the layer's log table

The root folder holds one folder per Lakehouse (<name>.Lakehouse/Files, /Tables) and a
medallion.json with the Lakehouse names. Commands find it from --root, the MEDALLION_ROOT
environment variable, or the current folder.
"""
import argparse
import dataclasses
import json
import os
import sys
from pathlib import Path

from . import __version__
from .deploy import LOGICAL, create_lakehouses, data_sets, deploy_local, find_repo, plan

CONFIG_FILE = "medallion.json"
LAYERS = ("bronze", "silver", "gold")
NOTEBOOK_NAMES = {"bronze": "feed_loader", "silver": "bronze_to_silver", "gold": "silver_to_gold"}
SUMMARY_COLUMNS = {
    "bronze": ["feed", "file", "mode", "status", "rows", "seconds", "error"],
    "silver": ["source_table", "target_table", "status", "rows_read", "rows_rejected",
               "duplicates_removed", "rows_inserted", "rows_updated", "rows_appended", "seconds", "error"],
    "gold": ["table", "type", "status", "rows_in", "inserted", "updated", "new_versions", "unchanged",
             "seconds", "error"],
}


# --- the root folder ---------------------------------------------------------------
def default_settings():
    return {"workspace": "local", "raw": "ls_raw", "silver": "ls_cleansed", "gold": "ls_gold", "schema": "dbo"}


def find_root(arg):
    root = Path(arg or os.environ.get("MEDALLION_ROOT") or ".").expanduser().resolve()
    if not (root / CONFIG_FILE).is_file():
        raise SystemExit(f"{root} is not a medallion folder (no {CONFIG_FILE}). "
                         f"Create one with: medallion init <folder>, or pass --root")
    return root


def read_settings(root):
    settings = default_settings()
    settings.update(json.loads((root / CONFIG_FILE).read_text(encoding="utf-8")))
    return settings


def names_of(settings):
    return dict(zip(LOGICAL, (settings["raw"], settings["silver"], settings["gold"])))


# --- parameters ----------------------------------------------------------------------
CLASSES = {"bronze": ("BronzeParams", "BronzeRun"), "silver": ("SilverParams", "SilverRun"),
           "gold": ("GoldParams", "GoldRun")}


def _layer(layer, which):
    from importlib import import_module
    return getattr(import_module(f"medallion.{layer}"), CLASSES[layer][which])


def params_class(layer):
    return _layer(layer, 0)


def run_class(layer):
    return _layer(layer, 1)


def lakehouse_params(layer, s):
    """Point a layer at the root's Lakehouses."""
    ws, schema = s["workspace"], s["schema"]
    return {
        "bronze": {"WORKSPACE": ws, "LAKEHOUSE": s["raw"], "SCHEMA": schema},
        "silver": {"SOURCE_WORKSPACE": ws, "SOURCE_LAKEHOUSE": s["raw"], "SOURCE_SCHEMA": schema,
                   "TARGET_WORKSPACE": ws, "TARGET_LAKEHOUSE": s["silver"], "TARGET_SCHEMA": schema},
        "gold": {"SILVER_WORKSPACE": ws, "SILVER_LAKEHOUSE": s["silver"], "SILVER_SCHEMA": schema,
                 "GOLD_WORKSPACE": ws, "GOLD_LAKEHOUSE": s["gold"], "GOLD_SCHEMA": schema},
    }[layer]


def coerce(text, default):
    """A --set value as the parameter's type (taken from its default)."""
    low = text.strip().lower()
    if low in ("none", "null"):
        return None
    if isinstance(default, bool):
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
        raise ValueError(f"expected true or false, got {text!r}")
    if isinstance(default, int):
        return int(text.replace("_", ""))
    return text


def parse_sets(items, layers):
    """--set [layer.]NAME=VALUE -> {layer: {NAME: value}}. Unprefixed names apply to every layer that has them."""
    out = {layer: {} for layer in layers}
    for item in items or []:
        if "=" not in item:
            raise SystemExit(f"--set needs NAME=VALUE, got {item!r}")
        name, value = item.split("=", 1)
        target, _, name = name.rpartition(".")
        name = name.strip().upper()
        targets = [target.lower()] if target else list(layers)
        if target and target.lower() not in layers:
            raise SystemExit(f"--set {item}: '{target}' is not one of the layers being run ({', '.join(layers)})")
        applied = False
        for layer in targets:
            fields = {f.name: f for f in dataclasses.fields(params_class(layer))}
            if name in fields:
                try:
                    out[layer][name] = coerce(value, fields[name].default)
                except ValueError as e:
                    raise SystemExit(f"--set {item}: {e}")
                applied = True
        if not applied:
            raise SystemExit(f"--set {item}: no parameter {name} in {', '.join(targets)} "
                             f"(see: medallion params <layer>)")
    return out


def build_params(layer, settings, args, sets):
    p = params_class(layer)()
    changes = dict(lakehouse_params(layer, settings))
    if args.dry_run:
        changes["DRY_RUN"] = True
    if getattr(args, "full_reload", False) and layer != "bronze":
        changes["FULL_RELOAD"] = True
    if getattr(args, "tables", None):
        changes["TABLES"] = args.tables
    if layer == "bronze":
        if getattr(args, "feeds", None):
            changes["FEED_FILTER"] = args.feeds
        if getattr(args, "folder", None):
            changes["FOLDER"] = args.folder
        if getattr(args, "table", None):
            changes["TABLE"] = args.table
    changes.update(sets.get(layer, {}))
    return p.updated(**changes)


# --- commands ------------------------------------------------------------------------
def cmd_init(args):
    root = Path(args.folder).expanduser().resolve()
    settings = default_settings()
    if (root / CONFIG_FILE).is_file():
        settings.update(json.loads((root / CONFIG_FILE).read_text(encoding="utf-8")))
    for k in ("raw", "silver", "gold"):
        if getattr(args, k):
            settings[k] = getattr(args, k)
    root.mkdir(parents=True, exist_ok=True)
    create_lakehouses(root, (settings["raw"], settings["silver"], settings["gold"]), settings["schema"])
    (root / CONFIG_FILE).write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")
    print(f"Lakehouses in {root}: {settings['raw']}, {settings['silver']}, {settings['gold']}")
    if not args.no_config or args.data:
        _deploy(root, settings, args.repo, args.data, include_config=not args.no_config)
    print(f"\nNext: medallion run all --root \"{root}\"")


def _deploy(root, settings, repo_arg, sets, include_config=True):
    repo = Path(repo_arg).resolve() if repo_arg else find_repo()
    if repo is None:
        raise SystemExit("Can't find the repository's config/ folder; pass --repo <path to the repo>")
    try:
        items = plan(repo, names_of(settings), sets, include_config)
    except ValueError as e:
        raise SystemExit(str(e))
    deploy_local(items, root)
    what = ("config" if include_config else "") + (f"{' and ' if include_config else ''}data {', '.join(sets)}" if sets else "")
    print(f"Copied {len(items)} file(s) ({what}) from {repo}")


def cmd_deploy(args):
    root = find_root(args.root)
    _deploy(root, read_settings(root), args.repo, args.data, include_config=not args.no_config)


def print_summary(layer, result):
    import polars as pl
    cols = [c for c in SUMMARY_COLUMNS[layer] if c in result.summary.columns]
    with pl.Config(tbl_rows=200, tbl_cols=len(cols), tbl_width_chars=200, fmt_str_lengths=60,
                   tbl_hide_dataframe_shape=True):
        print(result.summary.select(cols))


def cmd_run(args):
    from .storage import LocalStorage
    root = find_root(args.root)
    settings = read_settings(root)
    layers = list(LAYERS) if args.layer == "all" else [args.layer]
    if args.tables and len(layers) > 1:
        raise SystemExit("--tables names different things in silver and gold; with 'all', use "
                         "--set silver.TABLES=... / --set gold.TABLES=...")
    if (args.feeds or args.folder or args.table) and "bronze" not in layers:
        raise SystemExit("--feeds, --folder and --table are for bronze")
    sets = parse_sets(args.set, layers)
    params = {layer: build_params(layer, settings, args, sets) for layer in layers}   # all checked up front
    storage = LocalStorage(root)
    log = (lambda *_: None) if args.quiet else print
    exit_values, code = {}, 0
    for layer in layers:
        if not args.quiet:
            print(f"\n=== {layer} ({NOTEBOOK_NAMES[layer]}) " + "=" * 40)
        try:
            result = run_class(layer)(params[layer], storage, log=log).run()
        except Exception as e:     # a configuration error stops the run before any table loads
            print(f"{layer}: {type(e).__name__}: {e}", file=sys.stderr)
            code = 2
            break
        exit_values[layer] = result.exit_value
        if not args.json:
            print_summary(layer, result)
        if result.problems:
            print(f"{layer} FAILED: " + "; ".join(result.problems), file=sys.stderr)
            code = 1
            break                 # like a pipeline: later layers run only on success
    if args.json:
        print(json.dumps(exit_values if len(layers) > 1 else next(iter(exit_values.values()), {}),
                         indent=2, default=str))
    return code


def cmd_params(args):
    cls = params_class(args.layer)
    print(f"{args.layer} ({NOTEBOOK_NAMES[args.layer]}) parameters. Set with --set NAME=VALUE.\n")
    rows = [(f.name, repr(f.default), f.metadata.get("help", "")) for f in dataclasses.fields(cls)]
    wn, wv = max(len(r[0]) for r in rows), max(len(r[1]) for r in rows)
    for n, v, h in rows:
        print(f"  {n:<{wn}}  {v:<{wv}}  {h}")
    managed = ", ".join(lakehouse_params(args.layer, default_settings()))
    print(f"\nSet from medallion.json by the CLI: {managed}")


def cmd_log(args):
    import polars as pl
    from deltalake import DeltaTable
    root = find_root(args.root)
    s = read_settings(root)
    p = params_class(args.layer)()
    lakehouse = {"bronze": s["raw"], "silver": s["silver"], "gold": s["gold"]}[args.layer]
    table = args.table or p.LOG_TABLE
    path = root / f"{lakehouse}.Lakehouse" / "Tables" / (s["schema"] or "") / table
    if not DeltaTable.is_deltatable(str(path)):
        raise SystemExit(f"No log yet: {path}")
    df = pl.read_delta(str(path))
    order = next((c for c in ("run_started_utc", "rejected_utc", "processed_utc") if c in df.columns), df.columns[0])
    df = df.sort(order, descending=True).head(args.n)
    if args.all_columns or args.table:
        keep = df.columns
    else:
        keep = [c for c in ["run_started_utc"] + SUMMARY_COLUMNS[args.layer] if c in df.columns]
        if "run_started_utc" in keep:     # to the second, so the table fits on screen
            df = df.with_columns(pl.col("run_started_utc").dt.strftime("%Y-%m-%d %H:%M:%S"))
    with pl.Config(tbl_rows=args.n, tbl_cols=len(keep), tbl_width_chars=220, fmt_str_lengths=50,
                   tbl_hide_dataframe_shape=True):
        print(df.select(keep))


def cmd_maintain(args):
    from .storage import LocalStorage
    root = find_root(args.root)
    settings = read_settings(root)
    layers = list(LAYERS) if args.layer == "all" else [args.layer]
    for layer in layers:
        p = params_class(layer)().updated(**lakehouse_params(layer, settings))
        run_class(layer)(p, LocalStorage(root)).maintain()


# --- argument parsing --------------------------------------------------------------------
def parser():
    ap = argparse.ArgumentParser(prog="medallion", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", action="version", version=f"medallion {__version__}")
    sub = ap.add_subparsers(dest="command", required=True)
    repo = find_repo()
    sets_available = data_sets(repo) if repo else []

    p = sub.add_parser("init", help="create local Lakehouse folders and copy the config into them")
    p.add_argument("folder", help="folder to hold the Lakehouses (created if missing)")
    p.add_argument("--raw", help="bronze Lakehouse name (default ls_raw)")
    p.add_argument("--silver", help="silver Lakehouse name (default ls_cleansed)")
    p.add_argument("--gold", help="gold Lakehouse name (default ls_gold)")
    p.add_argument("--data", action="append", metavar="SET",
                   help=f"also copy a test data set into landing ({', '.join(sets_available) or 'none found'}); repeatable")
    p.add_argument("--no-config", action="store_true", help="don't copy config/ (keep the folder's own)")
    p.add_argument("--repo", help="repository to copy config/ and data/ from (default: the one this package is in)")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("deploy", help="copy config/ (and test data) from the repo into the root's Lakehouses again")
    p.add_argument("--root", help="medallion folder (default: MEDALLION_ROOT or the current folder)")
    p.add_argument("--data", action="append", metavar="SET", help="also copy a test data set; repeatable")
    p.add_argument("--no-config", action="store_true", help="data only")
    p.add_argument("--repo", help="repository to copy from")
    p.set_defaults(func=cmd_deploy)

    p = sub.add_parser("run", help="run bronze, silver, gold, or all three in order")
    p.add_argument("layer", choices=LAYERS + ("all",))
    p.add_argument("--root", help="medallion folder (default: MEDALLION_ROOT or the current folder)")
    p.add_argument("--dry-run", action="store_true", help="check everything, write nothing")
    p.add_argument("--full-reload", action="store_true", help="silver and gold: rebuild from all rows")
    p.add_argument("--tables", help="silver: bronze tables; gold: gold tables (comma-separated)")
    p.add_argument("--feeds", help="bronze: only these feeds (comma-separated)")
    p.add_argument("--folder", help="bronze: load one folder under Files, e.g. landing/orders")
    p.add_argument("--table", help="bronze, with --folder: the target table")
    p.add_argument("--set", action="append", metavar="[LAYER.]NAME=VALUE",
                   help="any notebook parameter, e.g. --set BATCH_ROWS=5000000 or --set gold.TABLES=dim_date; repeatable")
    p.add_argument("--json", action="store_true", help="print the notebook exit value(s) as JSON instead of tables")
    p.add_argument("-q", "--quiet", action="store_true", help="no progress messages")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("params", help="list a layer's parameters, defaults and meanings")
    p.add_argument("layer", choices=LAYERS)
    p.set_defaults(func=cmd_params)

    p = sub.add_parser("log", help="show recent runs from a layer's log table")
    p.add_argument("layer", choices=LAYERS)
    p.add_argument("--root")
    p.add_argument("-n", type=int, default=20, help="rows to show (default 20)")
    p.add_argument("--table", help="another table instead of the log, e.g. silver_rejects")
    p.add_argument("--all-columns", action="store_true")
    p.set_defaults(func=cmd_log)

    p = sub.add_parser("maintain", help="compact and vacuum a layer's tables")
    p.add_argument("layer", choices=LAYERS + ("all",))
    p.add_argument("--root")
    p.set_defaults(func=cmd_maintain)
    return ap


def main(argv=None):
    # tables and messages use Unicode; don't fail on a Windows code page when output is redirected
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    args = parser().parse_args(argv)
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
