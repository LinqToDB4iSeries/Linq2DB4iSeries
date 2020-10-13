# iSeries DB2 Provider for Linq2DB

This is a provider for Linq2DB to allow access to DB2 running on an IBM iSeries (AS/400) server.

It has been tested on iSeries v7.1 & 7.3 with iAccess for Windows version V7R1M0 with service pack SI55797

## Installation

Installing the Linq2Db4iSeries NuGetPackage will automatically install the Linq2Db package.

### Prerequisits

The IBM ADO.net providers are include with iSeries Access Client Solutions for Windows package(https://www-01.ibm.com/marketing/iwm/platform/mrs/assets?source=swg-ia) which will need to be installed onto each machine that runs the software.

### Providers

This package includes 4 Linq2db DataProviders, each based on one of the three .net data providers included in the afformentioned IBM provider package

- Access Client native Ado.Net provider
This provider provides the great compatibility but is only available for .net framework. This is the only provider supported on versions up to 2.9.x

- Access Client ODBC provider
This provider is advertised by IBM as the most efficient and does seem to be faster than the .net native provider. However it does not support the XML data type properly. Specifically, any schema calls on datareaders that access an XML column throw an exception. There are a few workarounds applied that will make most scenarios work but there are others that break. 

- Access Client OleDb provider
This provider is similar to the ODBC provider but fails on x86 and does not support transactions and has a few other quircks. For example it returns fixed length graphic datatypes trimmed. 

- DB2 provider (via DB2Connect)
This provider uses the same interface as the standard DB2 provider and supports .net framework and .net core. It is feature rich, maintained by IBM and is available through nuget. Hoewever it requires a commercial license.

The recommended provider is DB2Connect if a license can be obtained. Otherwise the Native .net provider is great if you're still on .net framework. The ODBC provider is a good choice if you want .net core/standard compatibility but cannot get DB2Connect license. Use the OleDb driver as a last resort if you don't care about transactions and x86 and you need to access columns with the XML data type.

For more info on IBM's comments on the ODBC and OleDb providers see: https://www.ibm.com/support/pages/oledb-ole-db-and-odbc-positioning

## Usage

You will need to add a linq2db section to your app.config/web.config file in order to add the DB2iSeries provider into Linq2DB

```xml
<configSections>
	<section name="linq2db" type="LinqToDB.Configuration.LinqToDBSection, linq2db" requirePermission="false"/>
</configSections>

<linq2db>
	<dataProviders>
		<add name="iSeriesProvider" type="LinqToDB.DataProvider.DB2iSeries.DB2iSeriesFactory, LinqToDB.DataProvider.DB2iSeries" default="true"/>
	</dataProviders>
</linq2db>
```

The data provider name (e.g. iSeriesProvider) is what you will use as the providerName in you connection string.

The rest of the usage is exactly the same as Linq2DB. See https://github.com/linq2db/linq2db/blob/master/README.md for further examples.

### Connection Strings

- Access Client native Ado.Net provider
Data Source={SERVER_NAME}; Persist Security Info=True;User ID={USER_ID};Password={PASSWORD};Library List={LIBRARY_LIST};Default Collection={DEFAULT_LIBRARY};Naming=0"

- Access Client ODBC provider
Driver={IBM i Access ODBC Driver};System=pub400.com;Uid={USER_ID};Pwd={PASSWORD};NAM=0;UNICODESQL=1;MAXDECSCALE=63;MAXDECPREC=63;GRAPHIC=1;MAPDECIMALFLOATDESCRIBE=3;MAXFIELDLEN=2097152;DBQ={LIBRARY_LIST}

For more info see: https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzaik/connectkeywords.htm#connectkeywords__note1

- Access Client OleDb provider
Provider=IBMDA400;Data Source=pub400.com;User Id={USER_ID};Password={PASSWORD};Default Collection={DEFAULT_LIBRARY};Convert Date Time To Char=FALSE;LIBRARY LIST={LIBRARY_LIST};Maximum Decimal Precision=63;Maximum Decimal Scale=63;Naming Convention=0

For more info see: https://www.ibm.com/support/pages/access-client-solutions-ole-db-custom-connection-properties

- DB2 provider (via DB2Connect)
Database={SERVER_NAME};User ID={USER_ID};Password={PASSWORD};Server={SERVER_NAME}:{SERVER_PORT};LibraryList={LIBRARY_LIST}

## Options

### Minimum DB2 Version
The provider can create SQL compatible with V7.1 and above.  

V7.1 PTF Level 38, V7.2 PTF Level 9 and V7.3 introduced a proper syntax for SKIP (OFFSET n ROWS). To have the provider generate this new syntax add MinVer="7.1.38" to the Provider or, if instantiating the provider in code set the parameter appropriatly.

### GUIDs
DB2 doesn't have a GUID type.  By default GUIDs will be stored as CHAR(16) FOR BIT DATA.  This works and is probably the most efficient however it is unreadable when queried directly.

Setting MapGuidAsString="true" (or setting the parameter true in the constructor) will save the GUID in clear text. The underlying column should be set to VARCHAR(38) data type.


## Caveats

1. The SchemaProvider implementation only returns details of the objects within the library list configured in the connection string even if the iSeries configuration for the user has other default Libraries specfied
2. Any objects created using this provider (e.g. as the result of a Create table statement) will be created in the Library in the Default Collection connection string parameter.  If this is not specifed then it wil be created in the default library for the user account.
3. Transactions can only be used if journalling is set up on the table (file).  If the iSeries schema is created with a CREATE SCHEMA command then this will be set by default however if the schema is created using the iSeries commands then you will need to either add journalling to the table explicitly OR create a journal receiver called QSQJRN in the Library to have journalling automatically applied to each table.

See https://github.com/LinqToDB4iSeries/Linq2DB4iSeries/wiki for further information.

## Breaking changes from v2.x to v3.x
1. Default naming convention is now SQL, please adjust your connection strings
