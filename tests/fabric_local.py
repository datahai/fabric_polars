"""Run the Fabric notebooks locally against folders laid out like Lakehouses.

A minimal stand-in for `notebookutils` is installed, each notebook's code cells are executed
in order, the parameters cell is overridden, and the OneLake base paths are redirected to
local folders. Nothing here talks to Fabric.
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
    """A folder per Lakehouse: <root>/<name>.Lakehouse/{Files,Tables/dbo}."""

    def __init__(self, root, raw="ls_raw", silver="ls_cleansed", gold="ls_gold"):
        self.root = Path(root)
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

    # which notebook variables hold OneLake base paths, and which Lakehouse each should point at
    def _bases(self):
        return {"BASE": self.raw, "SOURCE_BASE": self.raw, "TARGET_BASE": self.silver,
                "SILVER_BASE": self.silver, "GOLD_BASE": self.gold}

    def run(self, notebook, **params):
        """Execute a notebook's code cells. Returns (exit value or None, the notebook's globals)."""
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
                    g.update(params)
                if not redirected and any(isinstance(g.get(v), str) and g[v].startswith("abfss://")
                                          for v in self._bases()):
                    for var, lh in self._bases().items():
                        if isinstance(g.get(var), str) and g[var].startswith("abfss://"):
                            g[var] = str(self.lakehouse(lh))
                    g["storage_options"] = lambda: None
                    redirected = True
        except NotebookExit as e:
            return json.loads(e.value), g
        return None, g
