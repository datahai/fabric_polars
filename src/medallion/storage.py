"""Where the Lakehouses live.

Every layer reads and writes through a storage object, so the same code runs:
- in a Fabric notebook, against OneLake (FabricStorage, using notebookutils);
- on a PC, against local folders laid out like Lakehouses (LocalStorage).

A storage object gives each Lakehouse a base path (an abfss:// URL or a local folder), the
storage_options delta-rs and Polars need for that path, and the few file operations the
layers use: list a folder, read a small text file, move a file.
"""
import os
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Entry:
    """One item in a folder listing."""
    name: str
    path: str
    is_dir: bool
    size: int


def _enc(name):
    return name.replace(" ", "%20")   # spaces in workspace/lakehouse names must be encoded


def tables_root(base, schema):
    return f"{base}/Tables/{schema}" if schema else f"{base}/Tables"


class FabricStorage:
    """OneLake, from inside a Fabric notebook (authenticates with notebookutils)."""
    kind = "fabric"

    def __init__(self):
        import notebookutils
        self._nu = notebookutils

    def lakehouse(self, workspace, lakehouse):
        return f"abfss://{_enc(workspace)}@onelake.dfs.fabric.microsoft.com/{_enc(lakehouse)}.Lakehouse"

    def options(self):
        # fetched per call so long runs don't hit token expiry
        return {"bearer_token": self._nu.credentials.getToken("storage"),
                "use_fabric_endpoint": "true"}

    def ls(self, path):
        return [Entry(f.name, f.path, f.isDir, 0 if f.isDir else f.size) for f in self._nu.fs.ls(path)]

    def read_text(self, path, max_bytes=10_000_000):
        return self._nu.fs.head(path, max_bytes)

    def move(self, src, dest):
        self._nu.fs.mv(src, dest, True, True)    # create_path, overwrite

    def context(self):
        """Notebook and pipeline details from Fabric."""
        try:
            return dict(self._nu.runtime.context)
        except Exception:
            return {}


class LocalStorage:
    """Local folders: <root>/<lakehouse>.Lakehouse/{Files,Tables}. The workspace name is ignored."""
    kind = "local"

    def __init__(self, root):
        self.root = Path(root).expanduser().resolve()

    def lakehouse(self, workspace, lakehouse):
        return (self.root / f"{lakehouse}.Lakehouse").as_posix()

    def options(self):
        return None

    def ls(self, path):
        names = sorted(os.listdir(path))           # raises if the folder doesn't exist, like OneLake
        out = []
        for n in names:
            p = os.path.join(path, n)
            d = os.path.isdir(p)
            out.append(Entry(n, Path(p).as_posix(), d, 0 if d else os.path.getsize(p)))
        return out

    def read_text(self, path, max_bytes=10_000_000):
        with open(path, encoding="utf-8") as fh:
            return fh.read(max_bytes)

    def move(self, src, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if os.path.exists(dest):
            os.remove(dest)
        shutil.move(src, dest)

    def context(self):
        return {"currentNotebookName": "medallion (local)", "isForPipeline": False,
                "userName": os.environ.get("USERNAME") or os.environ.get("USER")}
