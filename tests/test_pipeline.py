"""End-to-end tests: landing CSVs -> bronze -> silver -> gold, run locally.

The workspace is built once per module: config and the clean sample data are deployed,
all three layers run, then the dirty data set is deployed and all three run again.
"""
import polars as pl
import pytest
from deltalake import DeltaTable

from fabric_local import LocalWorkspace

TS = pl.Datetime("us", "UTC")


@pytest.fixture(scope="module")
def ws(tmp_path_factory):
    w = LocalWorkspace(tmp_path_factory.mktemp("fabric"))
    w.deploy("sample")
    w.first = {nb: w.run(nb)[0] for nb in ("feed_loader", "bronze_to_silver", "silver_to_gold")}
    w.gold_v1 = {t: DeltaTable(w.table(w.gold, t)).version() for t in ("dim_customer", "fact_orders")}
    w.deploy("dirty")
    w.second = {nb: w.run(nb)[0] for nb in ("feed_loader", "bronze_to_silver", "silver_to_gold")}
    return w


def by(exit_value, key, name):
    return next(r for r in exit_value[key] if r.get("feed", r.get("source_table", r.get("table"))) == name)


# ---------------------------------------------------------------- bronze
def test_bronze_loads_each_feed_with_strict_types(ws):
    orders = pl.read_delta(ws.table(ws.raw, "orders_raw"))
    assert orders.schema["order_id"] == pl.Int64
    assert orders.schema["order_date"] == pl.Date
    assert orders.schema["amount"] == pl.Decimal(18, 2)
    assert orders.schema["_load_ts"] == TS
    counts = {f["feed"]: f["rows"] for f in ws.first["feed_loader"]["files"] if f["status"] == "loaded"}
    assert counts == {"customers": 1000, "orders": 5000, "products": 200}


def test_bronze_archives_loaded_files_and_logs_them(ws):
    archive = ws.lakehouse(ws.raw) / "Files" / "archive"
    assert sorted(p.name for p in archive.rglob("*.csv")) == sorted([
        "customers_20261015.csv", "orders_20261015.csv", "products_20261015.csv",
        "customers_20261021_dirty.csv", "orders_20261021_dirty.csv", "products_20261021_dirty.csv"])
    assert not list((ws.lakehouse(ws.raw) / "Files" / "landing").rglob("*.csv"))
    log = pl.read_delta(ws.table(ws.raw, "load_log"))
    assert log.filter(pl.col("status") == "loaded").height == 6


def test_bronze_skips_a_file_already_loaded(ws):
    src = next((ws.lakehouse(ws.raw) / "Files" / "archive").rglob("orders_20261015.csv"))
    dest = ws.lakehouse(ws.raw) / "Files" / "landing" / "orders" / src.name
    dest.write_bytes(src.read_bytes())
    before = pl.read_delta(ws.table(ws.raw, "orders_raw")).height
    out, _ = ws.run("feed_loader", FEED_FILTER="orders")
    assert by(out, "files", "orders")["status"] == "skipped"
    assert pl.read_delta(ws.table(ws.raw, "orders_raw")).height == before


# ---------------------------------------------------------------- silver
def test_silver_applies_rules_to_the_dirty_batch(ws):
    customers = by(ws.second["bronze_to_silver"], "tables", "customers_raw")
    orders = by(ws.second["bronze_to_silver"], "tables", "orders_raw")
    assert (customers["rows_read"], customers["rows_rejected"], customers["duplicates_removed"]) == (440, 72, 34)
    assert (customers["rows_inserted"], customers["rows_updated"]) == (86, 248)
    assert (orders["rows_read"], orders["rows_rejected"], orders["rows_inserted"]) == (500, 52, 409)


def test_silver_data_is_clean(ws):
    c = pl.read_delta(ws.table(ws.silver, "customers"))
    allowed = {"United Kingdom", "Ireland", "France", "Germany", "Netherlands", "Spain", "United States", None}
    assert set(c["country"].unique().to_list()) <= allowed
    assert c["customer_name"].null_count() == 0
    assert c.filter(pl.col("customer_name").str.contains(r"\s{2,}|\p{C}")).height == 0
    assert c["customer_id"].n_unique() == c.height
    o = pl.read_delta(ws.table(ws.silver, "orders"))
    assert o["amount"].min() >= 0
    assert o["customer_id"].null_count() == 0


def test_silver_records_rejects_with_original_rows(ws):
    r = pl.read_delta(ws.table(ws.silver, "silver_rejects"))
    assert r.height > 0
    assert r["row_json"].str.starts_with("{").all()
    assert set(r["source_table"].unique()) <= {"customers_raw", "orders_raw"}


def test_silver_rerun_finds_nothing_new(ws):
    out, _ = ws.run("bronze_to_silver")
    assert {t["status"] for t in out["tables"]} == {"no new data"}


# ---------------------------------------------------------------- gold
def test_gold_dimension_history_is_consistent(ws):
    c = pl.read_delta(ws.table(ws.gold, "dim_customer")).filter(pl.col("customer_sk") != -1)
    current = c.filter("is_current")
    assert current.height == c["customer_id"].n_unique()                  # one current row per customer
    assert c["customer_sk"].n_unique() == c.height                         # keys unique
    v = c.sort("customer_id", "valid_from").with_columns(
        pl.col("valid_from").shift(-1).over("customer_id").alias("next_from"))
    assert v.filter(pl.col("next_from").is_not_null() & (pl.col("valid_to") != pl.col("next_from"))).height == 0
    assert c.group_by("customer_id").len().filter(pl.col("len") > 1).height > 0   # the dirty batch made versions


def test_gold_surrogate_keys_are_stable(ws):
    first = pl.read_delta(ws.table(ws.gold, "dim_customer"), version=0).select("customer_id", "customer_sk")
    now = (pl.read_delta(ws.table(ws.gold, "dim_customer")).sort("valid_from")
             .group_by("customer_id").first().select("customer_id", "customer_sk"))
    joined = first.join(now, on="customer_id", how="inner", suffix="_now")
    assert joined.filter(pl.col("customer_sk") != pl.col("customer_sk_now")).height == 0


def test_gold_facts_point_at_the_version_valid_on_the_order_date(ws):
    f = pl.read_delta(ws.table(ws.gold, "fact_orders"))
    o = pl.read_delta(ws.table(ws.silver, "orders"))
    c = pl.read_delta(ws.table(ws.gold, "dim_customer"))
    assert f.height == o.height
    x = (f.join(o.select("order_id", "customer_id", "order_date"), on="order_id")
          .join(c.select("customer_sk", "customer_id", "valid_from", "valid_to"), on="customer_sk", suffix="_dim"))
    d = pl.col("order_date").cast(TS)
    wrong = x.filter((pl.col("customer_sk") != -1) & ((pl.col("customer_id") != pl.col("customer_id_dim"))
                     | (d < pl.col("valid_from")) | (d >= pl.col("valid_to"))))
    assert wrong.height == 0


def test_gold_full_reload_rebuilds_the_same_facts(ws):
    before = pl.read_delta(ws.table(ws.gold, "fact_orders")).drop("_gold_updated_utc").sort("order_id")
    ws.run("silver_to_gold", FULL_RELOAD=True)
    after = pl.read_delta(ws.table(ws.gold, "fact_orders")).drop("_gold_updated_utc").sort("order_id")
    assert after.equals(before)


def test_gold_dry_run_writes_nothing(ws):
    versions = {t: DeltaTable(ws.table(ws.gold, t)).version() for t in ("dim_customer", "fact_orders")}
    ws.run("silver_to_gold", DRY_RUN=True, FULL_RELOAD=True)
    assert versions == {t: DeltaTable(ws.table(ws.gold, t)).version() for t in versions}


# ---------------------------------------------------------------- config checks
def test_config_mistakes_fail_before_loading(ws, tmp_path):
    sql = ws.lakehouse(ws.gold) / "Files" / "config" / "gold"
    bad = ws.lakehouse(ws.gold) / "Files" / "config" / "bad"
    bad.mkdir(exist_ok=True)
    for f in sql.glob("*.sql"):
        (bad / f.name).write_text(f.read_text().replace("-- scd: 1", "-- scd: 3"))
    with pytest.raises(ValueError, match="scd"):
        ws.run("silver_to_gold", SQL_FOLDER="config/bad", DRY_RUN=True)
