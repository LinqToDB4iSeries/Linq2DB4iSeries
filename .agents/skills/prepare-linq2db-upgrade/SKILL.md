---
name: prepare-linq2db-upgrade
description: Prepare a Linq2DB4iSeries version-upgrade branch by selecting an upstream release, syncing test infrastructure, and aligning dependencies. Use for upgrade preparation, not provider regression fixes or completing a successful build.
---

# Prepare a linq2db Upgrade

Prepare this repository for regression work against a selected linq2db release. Apply the repository's `AGENTS.md`. Creating this skill does not itself authorize running the upgrade workflow.

## Select the release and branch

1. Read `Directory.Packages.props`, root `Directory.Build.props`, the solution, test project files, and Git status. Record the current version, branch, and pre-existing changes. Never discard unrelated work or overwrite modified files during synchronization; resolve overlaps with the user first.
2. Query live GitHub releases for `linq2db/linq2db`, using a GitHub connector or `https://api.github.com/repos/linq2db/linq2db/releases/latest`. Offer the latest published stable release with its release link and current local version. Let the user accept it or specify any other version. If the invocation already specifies a version, honor it and verify the corresponding release tag. Wait for the selection before modifying the branch or dependencies; do not treat silence as acceptance.
3. Normalize the selected version by removing a leading `v` or `V`. Create branch `V{version}` from the current checkout, for example `V6.6.0`. If it exists, inspect it and reuse only if appropriate; never reset or recreate it destructively.
4. Obtain the companion clone of `https://github.com/linq2db/linq2db.git` at `$env:TEMP\linq2db_4i_{branchname}`. Sanitize separators and invalid filename characters in the branch name as `AGENTS.md` requires. Verify an existing clone's remote and preserve local changes. Fetch and check out the selected release tag in detached HEAD mode; verify HEAD resolves to that tag. Never sync from upstream HEAD. During this preparation, the selected release becomes the target in `Directory.Packages.props`; on completion its version and the companion tag must agree.

## Synchronize test infrastructure

Use only the selected companion checkout as source. Replace the contents of these four corresponding local directories with upstream files:

- `Tests/Base/`
- `Tests/Linq/`
- `Tests/FSharp/`
- `Tests/VisualBasic/`

Delete obsolete local files in those synchronization roots before copying, with these exclusions applied to **both deletion and copying**:

- Preserve all local project files (`*.csproj`, `*.fsproj`, `*.vbproj`) rather than copying upstream project files.
- Preserve `Tests/Base/CustomizationSupport/` and `Tests/Linq/Custom/` in full. The request's `Test/Custom` refers to this repository's `Tests/Linq/Custom/`.
- Preserve the entire `Tests/Compatibility/` project. Do not delete outside the four roots or implicitly refresh `Tests/Model/`.
- Exclude generated `bin/` and `obj/` content and local credentials/configuration such as `UserDataProviders*`; do not import upstream build outputs.

Before deletion, enumerate the actual files to replace and verify resolved absolute paths remain within the four intended roots, including checking for reparse points. Snapshot or hash protected files for comparison afterward. On Windows, use native PowerShell file operations end-to-end with literal paths; do not run an unfiltered mirror or recursive delete over `Tests/`. Copy source files and test resources, not just C# files. Verify obsolete upstream-derived files disappeared and copied files match upstream, apart from explicitly justified infrastructure adaptations.

## Preserve and reconcile project configuration

Do not replace local project files or import upstream target frameworks, output types, signing, compile exclusions, or project references. Only these project-file edits are allowed:

1. Add newly required upstream package references that are missing locally. Compare equivalent upstream projects and imported props, respecting target-framework conditions and avoiding duplicates. Use central versions rather than adding inline versions where central package management applies.
2. Update `Tests/FSharp/Tests.FSharp.fsproj` compile items to include all synchronized `.fs` files exactly once and remove stale entries. Follow upstream dependency/compilation order, including signature-file ordering where applicable; alphabetical order is insufficient.

Adapt shared test `.props`/`.targets` files only where package or infrastructure changes require it. Preserve IBM i provider wiring, compatibility support, serial execution settings, local target frameworks, and customization hooks. If a required project edit falls outside the two exceptions, report it for follow-up instead of silently expanding scope.

## Align central package versions

Compare local and companion `Directory.Packages.props` semantically, including property-expanded versions, conditional groups, and version properties.

- Set local `<linq2dbVersion>` to the selected version and align all linq2db package versions with that release. Keep intentional shared-property references.
- Update shared dependency versions to those from the selected upstream release and add newly included package entries and their required properties/conditions. Reconcile upstream property renames without leaving unresolved references.
- Preserve repository-specific package entries and central-management settings. Do not blindly replace the file or remove local-only dependencies.
- Align the repository's release `<Version>` in root `Directory.Build.props` with the selected upgrade version; preserve unrelated build properties.

## Verify infrastructure and stop before regression repair

Verify solution/project sanity: parse XML; check solution project paths, project references, imports, explicit source/resource paths, F# completeness/order, unique package entries in each applicable condition, and resolved package versions. Use MSBuild project evaluation and a scoped restore to detect dependency and infrastructure issues. Distinguish unavailable SDKs, drivers, network access, and credentials from repository defects.

Fix package/restore problems, missing dependencies, and failures caused by changed test infrastructure within the constraints above. A single diagnostic build may be used if needed to distinguish these from upstream API regressions; do not repeatedly build toward success. Do not run integration tests or test discovery that requires a successful compilation during preparation. Do not alter provider behavior, rewrite assertions, or suppress failing tests to repair linq2db API or behavioral regressions. Those failures are the intended next phase.

Check the final diff against the initial status and protected-file snapshot. Report the branch, release/tag, companion path, synchronized directories, dependency changes, checks performed, and remaining infrastructure blockers or API regressions. State explicitly whether a diagnostic build was attempted. Completion means the upgrade workspace is prepared and infrastructure checked, not that the solution builds successfully. Do not commit or push unless requested.
