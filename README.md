# iSeries DB2 Provider for Linq2DB

This is a provider for Linq2DB to allow access to DB2 running on an IBM iSeries (AS/400) server.

It has been tested on iSeries v7.2 with iAccess for Windows version V7R1M0 with service pack SI55797

## Installation

Installing the Linq2Db4iSeries NuGetPackage will automatically install the Linq2Db package.

### Prerequisits

IBM do not provide a standalone install of the IBM.Data.DB2.iSeries packages nor do they provide a NuGet package for them.  They are include with iSeries Access for Windows (http://www.ibm.com/support/knowledgecenter/ssw_ibm_i_72/rzaij/rzaijrzaijinstall.htm) which will need to be installed onto each machine that runs the software.

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

You will need to provide valid iSeries connection strings at the very least specifying Data source, User Id and Password. e.g. Data Source={SERVER_NAME}; Persist Security Info=True;User ID={USER_ID};Password={PASSWORD};Library List={LIBRARY_LIST};Default Collection={DEFAULT_LIBRARY};Naming=1"

The rest of the usage is exactly the same as Linq2DB. See https://github.com/linq2db/linq2db/blob/master/README.md for further examples.

## Caveats

1. The SchemaProvider implementation only returns details of the objects within the library list configured in the connection string even if the iSeries configuration for the user has other default Libraries specfied
2. Any objects created using this provider (e.g. as the result of a Create table statement) will be created in the Library in the Default Collection connection string parameter.  If this is not specifed then it wil be created in the default library for the user account.
3. Transactions can only be used if journalling is set up on the table (file).  If the iSeries schema is created with a CREATE SCHEMA command then this will be set by default however if the schema is created using the iSeries commands then you will need to either add journalling to the table explicitly OR create a journal receiver called QSQJRN in the Library to have journalling automatically applied to each table.

