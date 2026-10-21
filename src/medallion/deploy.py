"""Copy the repo's configuration (and test data) into Lakehouses: local folders or OneLake.

Repo layout -> Lakehouse:
    config/<logical lakehouse>/Files/...   -> <lakehouse>.Lakehouse/Files/...
    data/<set>/landing/<feed>/*.csv        -> <raw lakehouse>.Lakehouse/Files/landing/<feed>/

The folder names under config/ (ls_raw, ls_cleansed, ls_gold) are logical names, mapped to
real Lakehouse names by `names` ({"ls_raw": ..., "ls_cleansed": ..., "ls_gold": ...}).
"""
import shutil
from pathlib import Path

ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"
LOGICAL = ("ls_raw", "ls_cleansed", "ls_gold")


def find_repo(start=None):
    """The repository root (the folder holding config/ and data/), searching up from here."""
    here = Path(start or __file__).resolve()
    for p in [here, *here.parents]:
        if (p / "config" / "ls_raw").is_dir():
            return p
    return None


def data_sets(repo):
    root = Path(repo) / "data"
    return sorted(p.name for p in root.iterdir() if p.is_dir()) if root.is_dir() else []


def plan(repo, names, sets=(), include_config=True):
    """List of (local file, lakehouse, path under the Lakehouse item)."""
    repo = Path(repo)
    items = []
    if include_config:
        for logical in LOGICAL:
            root = repo / "config" / logical
            if not root.is_dir():
                continue
            for f in sorted(p for p in root.rglob("*") if p.is_file()):
                items.append((f, names[logical], f.relative_to(root).as_posix()))
    for data_set in sets or []:
        root = repo / "data" / data_set
        if not root.is_dir():
            raise ValueError(f"No data set '{data_set}' under data/ (available: {', '.join(data_sets(repo))})")
        for f in sorted(p for p in root.rglob("*") if p.is_file()):
            items.append((f, names["ls_raw"], "Files/" + f.relative_to(root).as_posix()))
    return items


def deploy_local(items, target):
    for src, lakehouse, rel in items:
        dest = Path(target) / f"{lakehouse}.Lakehouse" / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)


def deploy_onelake(items, workspace):
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        raise SystemExit("Install the Azure SDK first: pip install azure-identity azure-storage-file-datalake")
    service = DataLakeServiceClient(ONELAKE_URL, credential=DefaultAzureCredential())
    fs = service.get_file_system_client(workspace)          # the workspace is the file system
    for src, lakehouse, rel in items:
        client = fs.get_file_client(f"{lakehouse}.Lakehouse/{rel}")
        with open(src, "rb") as fh:
            client.upload_data(fh, overwrite=True)


def create_lakehouses(root, lakehouses, schema="dbo"):
    """Empty local Lakehouse folders: <root>/<name>.Lakehouse/{Files,Tables/<schema>}."""
    for lh in lakehouses:
        base = Path(root) / f"{lh}.Lakehouse"
        (base / "Files").mkdir(parents=True, exist_ok=True)
        (base / "Tables" / schema if schema else base / "Tables").mkdir(parents=True, exist_ok=True)
