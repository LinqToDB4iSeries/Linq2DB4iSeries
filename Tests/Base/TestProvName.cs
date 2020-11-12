using System.Linq;

namespace Tests
{
	public static class TestProvName
	{
		public const string SqlAzure = "SqlAzure";
		public const string MariaDB = "MariaDB";
		/// <summary>
		/// MySQL 5.5
		/// Features:
		/// - supports year(2) type
		/// - fractional seconds not supported
		/// </summary>
		public const string MySql55 = "MySql55";
		public const string Firebird3 = "Firebird3";
		public const string Northwind = "Northwind";
		public const string NorthwindSQLite = "Northwind.SQLite";
		public const string NorthwindSQLiteMS = "Northwind.SQLite.MS";
		public const string PostgreSQL10 = "PostgreSQL.10";
		public const string PostgreSQL11 = "PostgreSQL.11";
		public const string Oracle11Native = "Oracle.11.Native";
		public const string Oracle11Managed = "Oracle.11.Managed";

		public const string DB2iBase = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2;

		public const string DB2iNet = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_AccessClient_71;
		public const string DB2iNetGAS = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_AccessClient_71_GAS;
		public const string DB2iNet73 = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_AccessClient_73;
		public const string DB2iNet73GAS = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_AccessClient_73_GAS;
		public const string DB2iODBC = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_ODBC_71;
		public const string DB2iOleDb = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_OleDb_71;
		public const string DB2iDB2Connect = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_DB2Connect_71;
		public const string DB2iOleDb54 = LinqToDB.DataProvider.DB2iSeries.DB2iSeriesProviderName.DB2_OleDb_54;

		public static readonly string[] AlliSeriesArray = new[] { DB2iNet, DB2iNet73, DB2iNet73GAS, DB2iNetGAS, DB2iODBC, DB2iOleDb, DB2iDB2Connect, DB2iOleDb54 };
		public static readonly string AlliSeries = string.Join(",", AlliSeriesArray);

		public static bool IsiSeries(string provider) => provider.StartsWith(DB2iBase);
		public static bool IsiSeriesODBC(string provider) => provider.StartsWith(DB2iBase) && provider.ToUpper().Contains("ODBC");
		public static bool IsiSeriesOleDb(string provider) => provider.StartsWith(DB2iBase) && provider.ToUpper().Contains("OLEDB");
		public static bool IsiSeriesDB2Connect(string provider) => provider.StartsWith(DB2iBase) && provider.ToUpper().Contains("CONNECT");
		public static bool IsiSeriesAccessClient(string provider) => IsiSeries(provider) && !IsiSeriesODBC(provider) && !IsiSeriesOleDb(provider) && !IsiSeriesDB2Connect(provider);
		public static string GetFamily(string provider)
		{
			if (IsiSeries(provider)
				|| new[] { LinqToDB.ProviderName.DB2, LinqToDB.ProviderName.DB2LUW, LinqToDB.ProviderName.DB2zOS }.Contains(provider))
				return LinqToDB.ProviderName.DB2;
			else
				return provider;
		}

		/// <summary>
		/// SQLite classic provider wrapped into MiniProfiler without mappings to provider types configured.
		/// Used to test general compatibility of linq2db with wrapped providers.
		/// </summary>
		public const string SQLiteClassicMiniProfilerUnmapped = "SQLite.Classic.MiniProfiler.Unmapped";
		/// <summary>
		/// SQLite classic provider wrapped into MiniProfiler with mappings to provider types configured.
		/// Used to test general compatibility of linq2db with wrapped providers.
		/// </summary>
		public const string SQLiteClassicMiniProfilerMapped = "SQLite.Classic.MiniProfiler.Mapped";


		/// <summary>
		/// Fake provider, which doesn't execute any real queries. Could be used for tests, that shouldn't be affected
		/// by real database access.
		/// </summary>
		public const string NoopProvider = "TestNoopProvider";

		public const string AllMySql = "MySql,MySqlConnector,MySql55,MariaDB";
		// MySql server providers (no mariaDB)
		public const string AllMySqlServer = "MySql,MySqlConnector,MySql55";
		// MySql <5.7 has inadequate FTS behavior
		public const string AllMySqlFullText = "MySql,MySqlConnector,MariaDB";
		public const string AllMySql57Plus = "MySql,MySqlConnector,MariaDB";
		// MySql server providers (no mariaDB) without MySQL 5.5
		public const string AllMySqlServer57Plus = "MySql,MySqlConnector";
		// MySql.Data server providers (no mysqlconnector)
		public const string AllMySqlData = "MySql,MySql55,MariaDB";
		public const string AllPostgreSQL = "PostgreSQL,PostgreSQL.9.2,PostgreSQL.9.3,PostgreSQL.9.5,PostgreSQL.10,PostgreSQL.11";
		public const string AllPostgreSQLv3 = "PostgreSQL,PostgreSQL.9.2,PostgreSQL.9.3,PostgreSQL.9.5,PostgreSQL.10,PostgreSQL.11";
		public const string AllPostgreSQLLess10 = "PostgreSQL,PostgreSQL.9.2,PostgreSQL.9.3,PostgreSQL.9.5";
		public const string AllPostgreSQL93Plus = "PostgreSQL,PostgreSQL.9.3,PostgreSQL.9.5,PostgreSQL.10,PostgreSQL.11";
		public const string AllPostgreSQL95Plus = "PostgreSQL,PostgreSQL.9.5,PostgreSQL.10,PostgreSQL.11";
		public const string AllPostgreSQL10Plus = "PostgreSQL.10,PostgreSQL.11";
		public const string AllOracle = "Oracle.Native,Oracle.Managed,Oracle.11.Native,Oracle.11.Managed";
		public const string AllOracleManaged = "Oracle.Managed,Oracle.11.Managed";
		public const string AllOracleNative = "Oracle.Native,Oracle.11.Native";
		public const string AllOracle11 = "Oracle.11.Native,Oracle.11.Managed";
		public const string AllOracle12 = "Oracle.Native,Oracle.Managed";
		public const string AllFirebird = "Firebird,Firebird3";
		public const string AllSQLite = "SQLite.Classic,SQLite.MS,SQLite.Classic.MiniProfiler.Unmapped,SQLite.Classic.MiniProfiler.Mapped";
		public const string AllSQLiteClassic = "SQLite.Classic,SQLite.Classic.MiniProfiler.Unmapped,SQLite.Classic.MiniProfiler.Mapped";
		public const string AllSybase = "Sybase,Sybase.Managed";
		public const string AllSqlServer = "SqlServer.2000,SqlServer.2005,SqlServer.2008,SqlServer.2012,SqlServer.2014,SqlServer.2017,SqlAzure";
		public const string AllSqlServer2005Minus = "SqlServer.2000,SqlServer.2005";
		public const string AllSqlServer2008Minus = "SqlServer.2000,SqlServer.2005,SqlServer.2008";
		public const string AllSqlServer2005Plus = "SqlServer.2005,SqlServer.2008,SqlServer.2012,SqlServer.2014,SqlServer.2017,SqlAzure";
		public const string AllSqlServer2008Plus = "SqlServer.2008,SqlServer.2012,SqlServer.2014,SqlServer.2017,SqlAzure";
		public const string AllSqlServer2012Plus = "SqlServer.2012,SqlServer.2014,SqlServer.2017,SqlAzure";
		public const string AllSqlServer2016Plus = "SqlServer.2017,SqlAzure";
		public const string AllSqlServer2017Plus = "SqlServer.2017";
		public const string AllSQLiteNorthwind = "Northwind.SQLite,Northwind.SQLite.MS";
		public const string AllSapHana = "SapHana.Native,SapHana.Odbc";
		public const string AllInformix = "Informix,Informix.DB2";
		public const string AllAccess = "Access,Access.Odbc";
	}
}
