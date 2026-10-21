# Fabric Git integration

Fabric can sync a workspace with a folder of a GitHub (or Azure DevOps) repository, so notebook changes made in Fabric are committed to Git and changes pushed to Git can be pulled into the workspace. It versions **items** (notebooks, pipelines, Lakehouse definitions), not Lakehouse **contents**: files under `Files/` and the tables are not synced. That's why this repo keeps the configuration in `config/` and deploys it with `scripts/deploy_to_onelake.py`.

## Two copies of the notebooks, one source of truth

| Folder | Format | Who writes it |
| --- | --- | --- |
| `notebooks/` | Standard Jupyter `.ipynb` | You. Portable: import into Fabric, open in VS Code or Jupyter, run the local tests |
| `fabric/` (after you connect) | Fabric's item format: one `<name>.Notebook/` folder with `notebook-content.py` and `.platform` | Fabric, when you commit from the workspace |

Pick one as the source of truth:

- **Notebooks edited in Fabric** (most teams): connect the workspace to `fabric/`, edit and commit in Fabric. Refresh `notebooks/` from Fabric when you want the local tests to cover your changes (download each notebook as `.ipynb`, or export it with the Fabric REST API).
- **Code edited locally** (recommended now that the notebooks are generated): change `src/medallion/`, run `python scripts/build_notebooks.py`, and import the changed notebooks into Fabric (or automate that with the Fabric REST API's item definition endpoints). Don't connect Git integration to `notebooks/`.

The engine cells of each notebook are generated from `src/medallion/`. If you edit them in Fabric, copy the change back into `src/` before the next regeneration, or it will be lost.

## Connect the workspace

You need to be a workspace admin, and your tenant must allow Git sync with GitHub (tenant settings *Users can synchronize workspace items with their Git repositories* and *Users can synchronize workspace items with GitHub repositories*).

1. Import the notebooks from `notebooks/` into the workspace, and create the Lakehouses and the pipeline.
2. **Workspace settings > Git integration > GitHub**, sign in, and choose this repository, the branch (e.g. `main`) and the folder `fabric`.
3. **Connect and sync.** If the `fabric/` folder is empty, the workspace's items are written to it. Commit from the workspace's **Source control** panel.

From then on, edit in Fabric and commit from **Source control**, or push to the branch and choose **Update all** in the workspace.

## Things to know

- **Lakehouse binding.** A notebook keeps the ID of its default Lakehouse in its metadata. These notebooks use full OneLake paths and don't need one, which keeps them portable between workspaces.
- **Outputs aren't committed.** Cell outputs never go to Git.
- **Separate workspaces per environment.** For dev, test and production, connect each workspace to its own branch, or use deployment pipelines; set the workspace and Lakehouse names through pipeline base parameters rather than editing the notebooks.
- **Config changes deploy separately.** A change under `config/` reaches the Lakehouses through the deploy script or the `deploy config to OneLake` workflow, not through Git sync.

Sources: [Get started with Git integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started) · [Notebook source control and deployment](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-source-control-deployment) · [What gets synced for a Lakehouse (Fabric Community)](https://community.fabric.microsoft.com/t5/Data-Engineering/Fabric-Lakehouse-What-metadata-gets-synced-to-Git-repository/td-p/3944370)
