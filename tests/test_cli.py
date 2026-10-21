"""The `medallion` command, the generated notebooks, and a few config parsers on their own."""
import dataclasses
import json
import subprocess
import sys

import polars as pl
import pytest

from fabric_local import REPO   # also puts src/ on sys.path

from medallion import cli, bronze, gold, silver


def medallion(*args):
    return cli.main([str(a) for a in args])


@pytest.fixture(scope="module")
def root(tmp_path_factory):
    r = tmp_path_factory.mktemp("cli")
    assert medallion("init", r, "--data", "sample") == 0
    return r


def test_init_creates_lakehouses_and_settings(root):
    settings = json.loads((root / "medallion.json").read_text())
    assert settings["raw"] == "ls_raw"
    assert (root / "ls_raw.Lakehouse" / "Files" / "landing" / "orders" / "_schema.json").is_file()
    assert (root / "ls_gold.Lakehouse" / "Files" / "config" / "gold" / "fact_orders.sql").is_file()
    assert (root / "ls_cleansed.Lakehouse" / "Tables" / "dbo").is_dir()


def test_run_all_then_dirty_data(root, capsys):
    assert medallion("run", "all", "--root", root, "-q") == 0
    assert pl.read_delta(str(root / "ls_gold.Lakehouse/Tables/dbo/fact_orders")).height == 5000
    assert medallion("deploy", "--root", root, "--data", "dirty", "--no-config") == 0
    capsys.readouterr()
    assert medallion("run", "all", "--root", root, "-q", "--json") == 0
    out = json.loads(capsys.readouterr().out)
    customers = next(t for t in out["silver"]["tables"] if t["source_table"] == "customers_raw")
    assert (customers["rows_rejected"], customers["duplicates_removed"]) == (72, 34)


def test_set_parameters_with_types(root):
    args = cli.parser().parse_args(["run", "silver", "--set", "BATCH_ROWS=5_000", "--set", "silver.FULL_RELOAD=true",
                                    "--set", "ROW_GROUP_ROWS=None"])
    sets = cli.parse_sets(args.set, ["silver"])
    p = cli.build_params("silver", cli.read_settings(root), args, sets)
    assert (p.BATCH_ROWS, p.FULL_RELOAD, p.ROW_GROUP_ROWS) == (5000, True, None)
    assert p.SOURCE_LAKEHOUSE == "ls_raw" and p.TARGET_LAKEHOUSE == "ls_cleansed"


def test_unknown_parameter_is_refused(root):
    with pytest.raises(SystemExit, match="no parameter NOPE"):
        medallion("run", "gold", "--root", root, "--set", "NOPE=1")


def test_dry_run_writes_nothing(root):
    log = root / "ls_gold.Lakehouse/Tables/dbo/gold_load_log"
    before = pl.read_delta(str(log)).height
    assert medallion("run", "gold", "--root", root, "--dry-run", "--full-reload", "-q") == 0
    assert pl.read_delta(str(log)).height == before


def test_config_error_returns_nonzero(root):
    bad = root / "ls_gold.Lakehouse/Files/config/bad"
    bad.mkdir()
    (bad / "dim_x.sql").write_text("-- type: dimension\nSELECT 1 AS id")     # no business key
    assert medallion("run", "gold", "--root", root, "--set", "SQL_FOLDER=config/bad", "-q") == 2


def test_log_command(root, capsys):
    assert medallion("log", "bronze", "--root", root, "-n", "3") == 0
    assert "customers" in capsys.readouterr().out


def test_root_is_required():
    with pytest.raises(SystemExit, match="not a medallion folder"):
        cli.find_root(str(REPO / "docs"))


# ---------------------------------------------------------------- generated notebooks
def test_notebooks_are_up_to_date():
    """notebooks/*.ipynb must match src/medallion; regenerate with scripts/build_notebooks.py."""
    r = subprocess.run([sys.executable, str(REPO / "scripts" / "build_notebooks.py"), "--check"],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


@pytest.mark.parametrize("notebook, cls", [("feed_loader", bronze.BronzeParams),
                                          ("bronze_to_silver", silver.SilverParams),
                                          ("silver_to_gold", gold.GoldParams)])
def test_notebook_parameter_cell_matches_the_engine(notebook, cls):
    nb = json.loads((REPO / "notebooks" / f"{notebook}.ipynb").read_text(encoding="utf-8"))
    cell = next(c for c in nb["cells"] if "parameters" in c["metadata"].get("tags", []))
    g = {}
    exec(cell["source"], g)
    assert {k: v for k, v in g.items() if k.isupper()} == {f.name: f.default for f in dataclasses.fields(cls)}


# ---------------------------------------------------------------- parsers
def test_schema_types():
    assert bronze.parse_type("Decimal(18, 2)", "x") == pl.Decimal(18, 2)
    assert bronze.parse_type("int64", "x") == pl.Int64
    with pytest.raises(ValueError, match="unknown type"):
        bronze.parse_type("Money", "x")
    with pytest.raises(ValueError, match="no entry in 'formats'"):
        bronze.parse_schema('{"columns": {"d": "Date"}}', "f")


def test_rules_are_checked():
    with pytest.raises(ValueError, match="unknown cleanse rule"):
        silver.parse_rules('{"tables": {"t": {"cleanse": {"c": ["shout"]}}}}', "rules.json")
    with pytest.raises(ValueError, match="needs a 'default'"):
        silver.parse_rules('{"tables": {"t": {"allowed_values": {"c": {"on_fail": "default"}}}}}', "rules.json")


def test_sql_header_and_run_order():
    dim = gold.parse_sql_file("dim_a.sql", "-- type: dimension\n-- business_key: id\n-- scd: 2\nSELECT 1 AS id, 'x' AS name")
    assert (dim["sk"], dim["scd"]) == ("a_sk", 2)
    fact = gold.parse_sql_file("fact_b.sql", "-- type: fact\n-- business_key: id\nSELECT * FROM gold.dim_a")
    assert fact["depends_on"] == ["dim_a"] and fact["load"] == "merge"
    assert gold.run_order({"dim_a": dim, "fact_b": fact}, {"fact_b", "dim_a"}) == ["dim_a", "fact_b"]
    with pytest.raises(ValueError, match="unknown setting"):
        gold.parse_sql_file("x.sql", "-- type: fact\n-- grain: day\nSELECT 1")
