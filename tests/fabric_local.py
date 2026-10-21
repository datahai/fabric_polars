"""Run the Fabric notebooks locally against folders laid out like Lakehouses.

A minimal stand-in for `notebookutils` is installed, each notebook's code cells are executed
in order, the parameters cell is overridden, and the notebook's storage is pointed at local
folders. Nothing here talks to Fabric.
"""
import json
import os
import shutil
import subprocess
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
NOTEBOOKS = REPO / "notebooks"
sys.path.insert(0, str(REPO / "src"))      # the engine, without needing `pip install -e .`


class _FileInfo:
    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path)
        self.isDir = os.path.isdir(path)
        self.size = 0 if self.isDir else os.path.getsize(path)


def _mv(src, dest, create_path=False, overwrite=False):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    if os.path.exists(dest) and overwrite:
        os.remove(dest)
    shutil.move(src, dest)


class NotebookExit(Exception):
    def __init__(self, value):
        super().__init__(value)
        self.value = value


def install_notebookutils(notebook_name="local"):
    def _exit(value):
        raise NotebookExit(value)
    mod = types.SimpleNamespace(
        fs=types.SimpleNamespace(
            ls=lambda p: [_FileInfo(os.path.join(p, x)) for x in sorted(os.listdir(p))],
            head=lambda p, n: open(p, encoding="utf-8").read(n),
            mv=_mv,
        ),
        credentials=types.SimpleNamespace(getToken=lambda audience: "local"),
        runtime=types.SimpleNamespace(context={"currentNotebookName": notebook_name, "isForPipeline": False}),
        notebook=types.SimpleNamespace(exit=_exit),
    )
    sys.modules["notebookutils"] = mod
    return mod


class LocalWorkspace:
    """A folder per Lakehouse: <root>/<name>.Lakehouse/{Files,Tables/dbo}.

    mode "notebook" runs the generated notebooks cell by cell; mode "engine" calls the
    medallion package directly, as the `medallion` command does."""

    def __init__(self, root, raw="ls_raw", silver="ls_cleansed", gold="ls_gold", mode="notebook"):
        self.root = Path(root)
        self.mode = mode
        self.raw, self.silver, self.gold = raw, silver, gold
        for lh in (raw, silver, gold):
            (self.lakehouse(lh) / "Files").mkdir(parents=True, exist_ok=True)
            (self.lakehouse(lh) / "Tables" / "dbo").mkdir(parents=True, exist_ok=True)

    def lakehouse(self, name):
        return self.root / f"{name}.Lakehouse"

    def table(self, lakehouse, table):
        return str(self.lakehouse(lakehouse) / "Tables" / "dbo" / table)

    def deploy(self, *data_sets):
        """Deploy config (and data sets) with the repo's own deploy script."""
        cmd = [sys.executable, str(REPO / "scripts" / "deploy_to_onelake.py"), "--local", str(self.root),
               "--raw", self.raw, "--silver", self.silver, "--gold", self.gold]
        for d in data_sets:
            cmd += ["--data", d]
        subprocess.run(cmd, check=True, capture_output=True)

    def lakehouse_params(self, notebook):
        """The Lakehouse-name parameters that point a notebook at this workspace's Lakehouses."""
        return {"feed_loader": {"LAKEHOUSE": self.raw},
                "bronze_to_silver": {"SOURCE_LAKEHOUSE": self.raw, "TARGET_LAKEHOUSE": self.silver},
                "silver_to_gold": {"SILVER_LAKEHOUSE": self.silver, "GOLD_LAKEHOUSE": self.gold}}[notebook]

    def run(self, notebook, **params):
        """Run one layer by its notebook name. Returns (exit value, notebook globals or RunResult)."""
        if self.mode == "engine":
            return self.run_engine(notebook, **params)
        return self.run_notebook(notebook, **params)

    def run_engine(self, notebook, **params):
        from medallion import LocalStorage, bronze, silver, gold
        layer, cls, runner = {"feed_loader": (bronze, bronze.BronzeParams, bronze.BronzeRun),
                              "bronze_to_silver": (silver, silver.SilverParams, silver.SilverRun),
                              "silver_to_gold": (gold, gold.GoldParams, gold.GoldRun)}[notebook]
        p = cls().updated(**{**self.lakehouse_params(notebook), **params})
        result = runner(p, LocalStorage(self.root), log=lambda *_: None).run()
        result.check()
        return json.loads(result.exit_json()), result

    def run_notebook(self, notebook, **params):
        """Execute a notebook's code cells. Returns (exit value or None, the notebook's globals).

        Once the engine's storage cell has run, FabricStorage is swapped for LocalStorage on this
        workspace's folder, so the notebook reads and writes local Lakehouses instead of OneLake."""
        params = {**self.lakehouse_params(notebook), **params}
        install_notebookutils(notebook)
        nb = json.loads((NOTEBOOKS / f"{notebook}.ipynb").read_text(encoding="utf-8"))
        g = {"__name__": "__notebook__"}
        redirected = False
        try:
            for cell in nb["cells"]:
                if cell["cell_type"] != "code":
                    continue
                src = cell["source"] if isinstance(cell["source"], str) else "".join(cell["source"])
                exec(compile(src, f"{notebook}.ipynb", "exec"), g)
                if "parameters" in cell.get("metadata", {}).get("tags", []):
                    unknown = sorted(k for k in params if k not in g)
                    if unknown:
                        raise ValueError(f"{notebook} has no parameter(s) {unknown}")
                    g.update(params)
                if not redirected and "FabricStorage" in g:
                    local = g["LocalStorage"]
                    g["FabricStorage"] = lambda: local(self.root)
                    redirected = True
        except NotebookExit as e:
            return json.loads(e.value), g
        return None, g
