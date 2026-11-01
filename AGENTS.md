# Repository Guidelines

## Project Structure & Module Organization

This repository implements the IBM i (DB2 iSeries) provider for linq2db.

- `Source/ISeriesProvider/`: SQL generation, mapping, schema discovery, bulk copy, and driver adapters.
- `Tests/Linq/`: upstream-derived integration tests; place IBM i-specific tests in `Custom/`.
- `Tests/Base/`, `Tests/Model/`, and `Tests/Compatibility/`: shared infrastructure, models, and compatibility helpers; `Tests/FSharp/` and `Tests/VisualBasic/` cover language integration.
- `Data/Create Scripts/`: database setup SQL. `Redist/` holds driver assemblies; `Icons/` contains package artwork.
- `Directory.Packages.props` centralizes dependency versions; directory-level `.props` files configure targets and builds.

All test projects are copies of the linq2db repository, except their project files, which are adjusted to build here. The only project-exclusive test source files are under `Tests/Base/CustomizationSupport/` and `Tests/Linq/Custom/`. Preserve this separation when updating tests from upstream.

## Required Upstream Reference for Agents

Download and maintain a local checkout of `https://github.com/linq2db/linq2db.git` as a companion repository. Keep it accessible throughout work: this provider derives from linq2db and overrides selected behavior, so understanding the full implementation requires consulting upstream code.

Store the checkout in the user's temporary directory as `linq2db_4i_{branchname}` (on Windows, `$env:TEMP\linq2db_4i_{branchname}`). Use the current branch name, replacing path separators and invalid filename characters with underscores, to isolate working branches.

Always check out the release tag matching `<linq2dbVersion>` in this branch's `Directory.Packages.props`, such as `v6.5.1` for `6.5.1`. Clone if absent; otherwise verify the remote and fetch the required tag. Recheck the version when switching branches or updating dependencies, and keep the companion checkout at that tag rather than upstream's default branch. Preserve any local changes when maintaining an existing checkout.

## Build, Test, and Development Commands

Run commands from the repository root. Use an SDK supporting C# 14 and `.slnx` solutions.

- `dotnet restore linq2db4iSeries.slnx`: restore solution dependencies.
- `dotnet build Source/ISeriesProvider/LinqToDB.DataProvider.DB2iSeries.csproj -c Release`: build the provider for `net462`, `netstandard2.0`, and `net6.0`.
- `dotnet run --project Tests/Linq/Tests.csproj --framework net8.0 -- --list-tests`: discover tests through the NUnit MTP runner.
- `dotnet run --project Tests/Linq/Tests.csproj --framework net8.0 -- <test-selection-options>`: execute only an approved, narrowly targeted selection. Use the runner's supported filters; never omit test selection and inadvertently run the full suite. Use `net462` on Windows when relevant.

## Coding Style & Naming Conventions

Follow `.editorconfig`: tabs, CRLF line endings, and a final newline. Match surrounding C# code: braces on separate lines, PascalCase types and methods, and camelCase parameters and locals. Preserve `DB2iSeries` naming and feature-based partial files such as `DB2iSeriesSqlBuilder.Merge.cs`. Nullable reference types are enabled. Resolve applicable banned-API and NUnit analyzer diagnostics.

## Development Guidelines

- Try to override existing upstream functionality and avoid code duplication as much as possible. If upstream code needs to be duplicate as the only solution note it explicitly in the comments and refer to the original code.
- *DO NOT* edit the copied upstream test infrastructure files. All changes need to be done in the provider source code or the repository owned test infrastructure code. If test need to be edited due to incompatibility with IBM i the need to be excluded in TestsInterceptor and copied over to local infrastructure for changes. 
- If tests are deemed invalid for IBM i they need to be excluded in test infrastructure

## Testing Guidelines

Tests use NUnit and Shouldly. Add regression cases under `Tests/Linq/Custom/`, following `DB2iSeriesTests.<Feature>.cs` naming. Keep upstream test customizations in the interception infrastructure rather than broadly editing imported tests.

These are integration tests. UnlessIf the tests target the public IBM i host `PUB400.com`, availability is not guaranteed. The full suite may take hours to run. Obtain human developer approval before running tests, and target a single test or a very small number of tests addressing a specific issue. Do not launch the full suite as routine validation. If the test host is unavailable, stop and report it; do not retry tests on connectivity issues.

The gitignored `Tests/UserDataProviders.json` controls which providers and provider configurations are included in discovered tests. Consult the human developer before creating or changing this file, including credentials, enabled providers, or configurations. Do not alter it to expand discovery or work around test failures. For initial setup, it is based on `Tests/DataProviders.json`.

The test hots needs to have the required IBM drivers preinstalled (Access Client for Windows) and journaling should be enabled on the target IBM i schema for transaction tests. Separate schemas for GAS and non-GAS runs should also be in place. DB2Connect requires a license install and is not supported on PUB400 and on IBM i by default. If a license file exists it should be copied to the clidriver/license folder to work with any nuget version of the IBM DB2 provider.

Preserve `Tests/Serial.runsettings` and avoid concurrent runs against shared schemas. No numeric coverage threshold is configured.

The test `a_CreateData.CreateDatabase` sets up the database schema for tests. Ensure a succesfull run before working with tests. Ask the user first if it should be run or if the schema is already setup and its ok to ommit it. If tests fail with missing schema or tables try running this test once. If this test fails there stop and report, there is no point in running any other test.

When working on a failed test focus on on provider first and then move to others. Most issues are provider agnostic and fixing for one provider will work for the others, but it may be provider specific. Default target provider is ODBC but request the user to select which one to target first each time. Access Client provider is .net framework only.

## Commit & Pull Request Guidelines

Recent commits use short descriptive subjects, such as `Upgraded to linq2db v6.3.0`; follow that style. Keep changes focused. PR descriptions should explain the behavior change, link relevant issues, and report tested frameworks, drivers, and IBM i versions, including any untested combinations. Never commit connection credentials or license files.
