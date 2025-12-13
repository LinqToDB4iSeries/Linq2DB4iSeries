# iSeries DB2 Provider for Linq2DB

This is a provider for Linq2DB to allow access to DB2 running on an IBM iSeries (AS/400) server.

## Installation

Installing the Linq2Db4iSeries NuGetPackage will automatically install the Linq2Db package.

### Prerequisits

The IBM i Series ADO.net providers (Native, ODBC, Oledb) are included with iSeries Access Client Solutions for Windows package(https://www-01.ibm.com/marketing/iwm/platform/mrs/assets?source=swg-ia) which will need to be installed onto each machine that runs the software. The ODBC driver is also available for linux.

For the DB2 ADO.net providers there are the following options
- Full download of the IBM Data Server Client package (.net framework only - requires installation on each server and developer machine) 
- IBM.Data.DB.Provider nuget package (.net framework - versions 11.1 and 11.5 supported) 
- IBM.Data.DB2.Core nuget package (.net core -  versions 11.1 and 11.5 supported -  linux and macosx supported through IBM.Data.DB2.Core-lnx and IBM.Data.DB2.Core-osx)
- Net.IBM.Data.DB2 nuget package (.net - versions 11.5 and later supported - linux and macosx supported through Net.IBM.Data.Db2-lnx and Net.IBM.Data.Db2-osx)

For DB2Connect a license file is required (named db2consv_ee.lic). For the full IBM Data Server Client the license can be installed using the license manager application included with the package (db2licm -a <license_file>).
For the nuget packages, the license file should be placed in the clidriver/license folder under the application base folder.

To include DB2 drivers properly, check the linq2db.Providers.props files in this repository for the relevant target frameworks.

### Providers

This package includes 4 Linq2db DataProviders, each based on one of the three .net data providers included in the afformentioned IBM provider package

- Access Client native Ado.Net provider
This provider provides the great compatibility but is only available for .net framework. This is the only provider supported on versions up to 2.9.x

- Access Client ODBC provider
This provider is advertised by IBM as the most efficient and does seem to be faster than the .net native provider. However it does not support the XML data type properly. Specifically, any schema calls on datareaders that access an XML column throw an exception. There are a few workarounds applied that will make most scenarios work but there are others that break. 

- Access Client OleDb provider
This provider is similar to the ODBC provider but fails on x86 and has a few other quirks.

- DB2 provider (via DB2Connect)
This provider uses the same interface as the standard DB2 provider and supports .net framework and .net core (x64 only for core). It is feature rich, maintained by IBM and is available through nuget. However it requires a commercial license.

The recommended provider is DB2Connect if a license can be obtained, as it provides the best compatibility and is available through nuget packages. Otherwise the native .net provider is great if you're still on .net Framework. The OleDb and ODBC providers are a good choice if you want .net core/standard compatibility but cannot get a DB2Connect license. The ODBC provider has issues with XML columns and the OleDb provider only works on x64. The OleDb provider also had some SQL quirks that are handled in the library code (required spaces in specific places) so use with caution.

For more info on providers see the [provider known issue and quirks wiki artice](https://github.com/LinqToDB4iSeries/Linq2DB4iSeries/wiki/Underlying-ADO-providers-known-issues-and-quirks)

For more info on IBM's comments on the ODBC and OleDb providers see: https://www.ibm.com/support/pages/oledb-ole-db-and-odbc-positioning

## Usage

Usage is exactly the same as Linq2DB. See https://github.com/linq2db/linq2db/blob/master/README.md for examples.

Valid ProviderNames for this provider are defined as constants in the DB2iSeriesProviderName static class.

### Connection Strings

- Access Client native Ado.Net provider
```
Data Source={SERVER_NAME}; Persist Security Info=True;User ID={USER_ID};Password={PASSWORD};Library List={LIBRARY_LIST};Default Collection={DEFAULT_LIBRARY};Naming=0"
```

- Access Client ODBC provider
```
Driver={IBM i Access ODBC Driver};System={SERVER_NAME};Uid={USER_ID};Pwd={PASSWORD};NAM=0;UNICODESQL=1;MAXDECSCALE=63;MAXDECPREC=63;GRAPHIC=1;MAPDECIMALFLOATDESCRIBE=3;MAXFIELDLEN=2097152;ALLOWUNSCHAR=1;DBQ={LIBRARY_LIST}
```

For more info see: https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzaik/connectkeywords.htm#connectkeywords__note1

- Access Client OleDb provider
```
Provider=IBMDA400;Data Source={SERVER_NAME};User Id={USER_ID};Password={PASSWORD};Default Collection={DEFAULT_LIBRARY};Convert Date Time To Char=TRUE;LIBRARY LIST={LIBRARY_LIST};Maximum Decimal Precision=63;Maximum Decimal Scale=63;Naming Convention=0;Keep Trailing Blanks=TRUE
```

For more info see: https://www.ibm.com/support/pages/access-client-solutions-ole-db-custom-connection-properties

- DB2 provider (via DB2Connect)
```
Database={SERVER_NAME};User ID={USER_ID};Password={PASSWORD};Server={SERVER_NAME}:{SERVER_PORT};LibraryList={LIBRARY_LIST};CurrentSchema=
{DEFAULT_LIBRARY}
```

## Options

### Provider Type
Supported providers are:

- Access Client native .net provider (.net framework >= 4.6.2 only)
- Access Client ODBC provider
- Access Client OleDb provider
- DB2 provider (via DB2 Connect license)

### Minimum DB2 Version
The provider can create SQL compatible with V7.1 and above.  

- V7.4 introduced the DROP IF EXISTS syntax
- V7.3 introduced the OFFSET clause
- V7.2 introduced the Truncate Table syntax.

To have the provider support these features this new features select the appropriate version in the DB2iSeriesOptions.

### GUIDs
DB2 doesn't have a GUID type.  By default GUIDs will be stored as CHAR(16) FOR BIT DATA.  This works and is probably the most efficient however it is unreadable when queried directly.

Using a "*GAS" provider will save the GUID in clear text. The underlying column should be set to VARCHAR(38) data type.

### Table Hints
The provider supports the following table hints:
- OVERRIDING SYSTEM VALUE : Used to override system generated values on insert (e.g. for identity columns)
Usage : 
```
table.TableHint(DB2iSeriesTableHints.OverridingSystemValue).Insert(() => new TableEntity { ... });
```

## Caveats

1. The SchemaProvider implementation only returns details of the objects within the library list configured in the connection string even if the iSeries configuration for the user has other default Libraries specified
2. Any objects created using this provider (e.g. as the result of a Create table statement) will be created in the Library in the Default Collection connection string parameter.  If this is not specifed then it wil be created in the default library for the user account.
3. Transactions can only be used if journalling is set up on the table (file).  If the iSeries schema is created with a CREATE SCHEMA command then this will be set by default however if the schema is created using the iSeries commands then you will need to either add journalling to the table explicitly OR create a journal receiver called QSQJRN in the Library to have journalling automatically applied to each table.
4. Linq2db remote context and scaffolding features are not currently supported.

See https://github.com/LinqToDB4iSeries/Linq2DB4iSeries/wiki for further information.

