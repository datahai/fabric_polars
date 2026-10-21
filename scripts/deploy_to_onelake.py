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
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))     # works without installing the package

from medallion.deploy import data_sets, deploy_local, deploy_onelake, plan  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument("--workspace", help="Fabric workspace name (or ID) to deploy to")
    target.add_argument("--local", metavar="DIR", help="Copy to a local folder instead (for testing)")
    ap.add_argument("--raw", default="ls_raw", help="Bronze Lakehouse name (default ls_raw)")
    ap.add_argument("--silver", default="ls_cleansed", help="Silver Lakehouse name (default ls_cleansed)")
    ap.add_argument("--gold", default="ls_gold", help="Gold Lakehouse name (default ls_gold)")
    ap.add_argument("--data", action="append", choices=data_sets(REPO),
                    help="Also upload a test data set to the landing folders (repeatable)")
    ap.add_argument("--dry-run", action="store_true", help="List what would be uploaded; change nothing")
    args = ap.parse_args()

    names = {"ls_raw": args.raw, "ls_cleansed": args.silver, "ls_gold": args.gold}
    try:
        items = plan(REPO, names, args.data)
    except ValueError as e:
        sys.exit(str(e))
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
