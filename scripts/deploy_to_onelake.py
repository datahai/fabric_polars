#!/usr/bin/env python3
"""Upload the pipeline's configuration (and optionally sample data) to OneLake.

Fabric Git integration syncs notebooks but not Lakehouse files, so the schema files,
silver rules and gold SQL in this repo are deployed with this script.

Repo layout -> OneLake:
    config/<lakehouse>/Files/...      -> <workspace>/<lakehouse>.Lakehouse/Files/...
    data/<set>/landing/<feed>/*.csv   -> <workspace>/<raw lakehouse>.Lakehouse/Files/landing/<feed>/

The folder names under config/ (ls_raw, ls_cleansed, ls_gold) are logical names; map them
to your real Lakehouse names with --raw / --silver / --gold.

Authentication uses azure-identity's DefaultAzureCredential: `az login` locally, or the
AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET environment variables for a
service principal (which needs Contributor or higher on the workspace).

Examples:
    python scripts/deploy_to_onelake.py --workspace "My Workspace" --dry-run
    python scripts/deploy_to_onelake.py --workspace "My Workspace"
    python scripts/deploy_to_onelake.py --workspace "My Workspace" --data sample
    python scripts/deploy_to_onelake.py --local /tmp/onelake --data sample   # mirror to a local folder
"""
import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"


def plan(args):
    """List of (local file, lakehouse, path under the Lakehouse item)."""
    names = {"ls_raw": args.raw, "ls_cleansed": args.silver, "ls_gold": args.gold}
    items = []
    config = REPO / "config"
    for logical, real in names.items():
        root = config / logical
        if not root.is_dir():
            continue
        for f in sorted(p for p in root.rglob("*") if p.is_file()):
            items.append((f, real, f.relative_to(root).as_posix()))
    for data_set in args.data or []:
        root = REPO / "data" / data_set
        if not root.is_dir():
            sys.exit(f"No data set '{data_set}' under data/ (available: "
                     f"{', '.join(sorted(p.name for p in (REPO / 'data').iterdir() if p.is_dir()))})")
        for f in sorted(p for p in root.rglob("*") if p.is_file()):
            items.append((f, args.raw, "Files/" + f.relative_to(root).as_posix()))
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
        sys.exit("Install the Azure SDK first: pip install azure-identity azure-storage-file-datalake")
    service = DataLakeServiceClient(ONELAKE_URL, credential=DefaultAzureCredential())
    fs = service.get_file_system_client(workspace)          # the workspace is the file system
    for src, lakehouse, rel in items:
        client = fs.get_file_client(f"{lakehouse}.Lakehouse/{rel}")
        with open(src, "rb") as fh:
            client.upload_data(fh, overwrite=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument("--workspace", help="Fabric workspace name (or ID) to deploy to")
    target.add_argument("--local", metavar="DIR", help="Copy to a local folder instead (for testing)")
    ap.add_argument("--raw", default="ls_raw", help="Bronze Lakehouse name (default ls_raw)")
    ap.add_argument("--silver", default="ls_cleansed", help="Silver Lakehouse name (default ls_cleansed)")
    ap.add_argument("--gold", default="ls_gold", help="Gold Lakehouse name (default ls_gold)")
    ap.add_argument("--data", action="append", choices=["sample", "dirty"],
                    help="Also upload a test data set to the landing folders (repeatable)")
    ap.add_argument("--dry-run", action="store_true", help="List what would be uploaded; change nothing")
    args = ap.parse_args()

    items = plan(args)
    where = args.workspace or args.local
    for src, lakehouse, rel in items:
        print(f"{src.relative_to(REPO).as_posix():<60} -> {where}/{lakehouse}.Lakehouse/{rel}")
    if args.dry_run:
        print(f"Dry run: {len(items)} file(s) would be uploaded")
        return
    if args.local:
        deploy_local(items, args.local)
    else:
        deploy_onelake(items, args.workspace)
    print(f"Uploaded {len(items)} file(s)")


if __name__ == "__main__":
    main()
