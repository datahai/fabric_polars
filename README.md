# Fabric medallion pipeline with Polars and DuckDB

A bronze → silver → gold pipeline for Microsoft Fabric that runs in **Python notebooks** (no Spark). Each layer is driven by configuration files, and each run only processes what's new.

| Layer | Notebook | Engine | Configured by |
| --- | --- | --- | --- |
| Bronze | `notebooks/feed_loader.ipynb` | Polars | A `_schema.json` per landing folder: column names, types, date formats |
| Silver | `notebooks/bronze_to_silver.ipynb` | Polars | `silver_rules.json`: duplicate keys, cleansing rules, allowed values |
| Gold | `notebooks/silver_to_gold.ipynb` | DuckDB SQL | One `.sql` file per dimension or fact, with SCD1/SCD2 and surrogate keys |

All Delta reads and writes go through delta-rs (`deltalake`). The code lives once, in the `src/medallion` package: it's copied into the notebooks for Fabric, and the `medallion` command runs it on your PC against local folders laid out like Lakehouses.

## Repository layout

```
src/medallion/                  the engine: one copy of every function
  common.py, storage.py         shared helpers; OneLake (in Fabric) and local-folder storage
  bronze.py, silver.py, gold.py the three layers, each with a Params class and a run
  deploy.py, cli.py             copying config into Lakehouses; the `medallion` command
notebooks/                      the Fabric notebooks (.ipynb), GENERATED from src/ - don't edit the engine cells
  feed_loader.ipynb             bronze: landing CSVs -> raw tables
  bronze_to_silver.ipynb        silver: rules-driven cleansing
  silver_to_gold.ipynb          gold: dimensional model from SQL
  generate_test_csvs.ipynb      optional: writes ~1 GB test files straight into landing
config/                         deployed to Lakehouse Files, mirroring their paths
  ls_raw/Files/landing/<feed>/_schema.json
  ls_cleansed/Files/config/silver_rules.json
  ls_gold/Files/config/gold/*.sql
data/
  sample/landing/<feed>/*.csv   clean test data (1,000 customers, 5,000 orders, 200 products)
  dirty/landing/<feed>/*.csv    messy rows to exercise the silver rules
scripts/
  build_notebooks.py            regenerates notebooks/ from src/ and notebook_templates/
  notebook_templates/           each notebook's markdown and cell layout
  deploy_to_onelake.py          uploads config/ (and optionally data/) to OneLake
tests/                          end to end, through the notebooks and through the engine; the CLI
pyproject.toml                  `pip install -e .` installs the engine and the `medallion` command
docs/
  GUIDE.md                      full guide: setup, formats, orchestration, operations
  FABRIC_GIT.md                 using Fabric Git integration with this repo
.github/workflows/
  tests.yml                     local end-to-end tests on push and pull request
  deploy-config.yml             deploys config/ to OneLake
```

## Quick start

1. **Create three Lakehouses** in a Fabric workspace, with Lakehouse schemas enabled: `ls_raw`, `ls_cleansed`, `ls_gold`.
2. **Deploy the configuration and sample data:**
   ```bash
   pip install -r requirements-deploy.txt
   az login
   python scripts/deploy_to_onelake.py --workspace "<your workspace>" --data sample
   ```
   Lakehouses named differently? Add `--raw`, `--silver` and `--gold`. Use `--dry-run` to see what will be uploaded.
3. **Import the notebooks** from `notebooks/` into the workspace (Import > Notebook > From this computer) and check each is set to **Python**.
4. **Set the parameters** at the top of each notebook (workspace and Lakehouse names), then run `feed_loader`, `bronze_to_silver` and `silver_to_gold` in that order. Use `DRY_RUN = True` first for silver and gold.
5. **Try the rules:** `python scripts/deploy_to_onelake.py --workspace "<your workspace>" --data dirty`, then run all three again to see rejects, duplicates, SCD2 versions and incremental fact loads.

To schedule it, put the three notebooks in a Data Pipeline. See [docs/GUIDE.md](docs/GUIDE.md#pipeline-orchestration).

## Running on your PC

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1               # macOS/Linux: source .venv/bin/activate
pip install -e .
medallion init C:\lakehouses --data sample
medallion run all --root C:\lakehouses
```

`init` creates local `ls_raw`, `ls_cleansed` and `ls_gold` Lakehouse folders and copies `config/` (and the sample CSVs) into them. `medallion run bronze|silver|gold|all` takes the notebooks' parameters (`--dry-run`, `--tables`, `--set BATCH_ROWS=2000000`, ...); `medallion log`, `medallion params` and `medallion maintain` do what their names say. Details in [docs/GUIDE.md](docs/GUIDE.md#running-on-your-pc). Running locally against OneLake is the next step.

## Changing the pipeline

| To | Change |
| --- | --- |
| Add a feed | Add `config/ls_raw/Files/landing/<feed>/_schema.json`, deploy, and drop CSVs in `Files/landing/<feed>/` |
| Cleanse a new table | Add an entry to `config/ls_cleansed/Files/config/silver_rules.json` |
| Add a dimension or fact | Add a `.sql` file to `config/ls_gold/Files/config/gold/` |

The notebooks themselves don't change. Formats are documented in [docs/GUIDE.md](docs/GUIDE.md).

To change the code, edit `src/medallion/`, then regenerate the notebooks and test:

```bash
python scripts/build_notebooks.py
python -m pytest
```

CI fails if `notebooks/` is out of date with `src/`. See [Changing the engine](docs/GUIDE.md#changing-the-engine).

## Running the tests locally

The tests execute the generated notebooks against local folders laid out like Lakehouses, with a small stand-in for Fabric's `notebookutils`, then run the same checks through the engine directly (as the `medallion` command does). Configuration is deployed with the same script used for OneLake.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -e .
python -m pytest
```

They load the sample and dirty data through all three layers and check, among other things:

- strict bronze types, archiving and the duplicate-file guard;
- silver reject and deduplication counts, and clean output;
- gold SCD2 history: one current row per member, contiguous versions and stable surrogate keys;
- every fact pointing at the dimension version valid on its date;
- a full reload rebuilding identical facts, and a dry run writing nothing.

`requirements-legacy.txt` pins an older matched set (Polars 1.6, deltalake 0.19, DuckDB 1.1); CI runs both.

## GitHub Actions

- **tests** runs on every push and pull request: on Linux with current and legacy library versions, and on Windows with current ones. It also checks the notebooks are up to date with `src/`.
- **deploy config to OneLake** runs on pushes to `main` that change `config/`, or manually. It needs:
  - repository secrets `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_SECRET` for a service principal with Contributor (or higher) on the workspace. Your Fabric admin may also need to allow service principals to use Fabric APIs;
  - a repository variable `FABRIC_WORKSPACE`, plus optional `RAW_LAKEHOUSE`, `SILVER_LAKEHOUSE` and `GOLD_LAKEHOUSE`;
  - a GitHub environment named `fabric` (add required reviewers there if deployments need approval).

## Fabric Git integration

Fabric can sync notebooks and pipelines with a folder of this repo, but not Lakehouse files. Configuration therefore deploys separately. See [docs/FABRIC_GIT.md](docs/FABRIC_GIT.md).

## Requirements and status

- A Fabric capacity, and Python (not PySpark) notebooks. The notebooks print the library versions they find; pin newer ones with the optional `%pip` cell at the top of each.
- Tested end to end locally with Polars 1.6, 1.33 and 2.0, deltalake 0.19, 1.1 and 1.6, and DuckDB 1.1 and 1.5. The bronze notebook has been run in Fabric. Run silver and gold with `DRY_RUN = True` in your workspace first.
