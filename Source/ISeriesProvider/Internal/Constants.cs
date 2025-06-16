using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class Constants
	{
		public static class ProviderFlags
		{
			public const string MapGuidAsString = "MapGuidAsString";
			public const string SupportsOffsetClause = "SupportsOffsetClause";
			public const string SupportsTruncateTable = "SupportsTruncateTable";
			public const string SupportsNamedParameters = "SupportsNamedParameters";
			public const string SupportsMergeStatement = "SupportsMergeStatement";
			public const string SupportsNCharTypes = "SupportsNCharTypes";
			public const string SupportsDropTableIfExists = "SupportsDropTableIfExists";
			public const string SupportsArbitraryTimeStampPrecision = "SupportsArbitraryTimeStampPrecision";
			public const string SupportsTrimCharacters = "SupportsTrimCharacters";
		}

		public static class DbTypes
		{
			public const string Decimal = "DECIMAL";
			public const string Numeric = "NUMERIC";
			public const string Binary = "BINARY";
			public const string Blob = "BLOB";
			public const string Char = "CHAR";
			public const string Char16ForBitData = "CHAR(16) FOR BIT DATA";
			public const string Clob = "CLOB";
			public const string DataLink = "DATALINK";
			public const string DbClob = "DBCLOB";
			public const string Graphic = "GRAPHIC";
			public const string VarBinary = "VARBINARY";
			public const string VarChar = "VARCHAR";
			public const string VarGraphic = "VARGRAPHIC";
			public const string Integer = "INTEGER";
			public const string SmallInt = "SMALLINT";
			public const string BigInt = "BIGINT";
			public const string TimeStamp = "TIMESTAMP";
			public const string Date = "DATE";
			public const string Time = "TIME";
			public const string Varg = "VARG";
			public const string DecFloat = "DECFLOAT";
			public const string Float = "FLOAT";
			public const string Double = "DOUBLE";
			public const string Real = "REAL";
			public const string RowId = "ROWID";
			public const string VarBin = "VARBIN";
			public const string XML = "XML";
			public const string NChar = "NCHAR";
			public const string NVarChar = "NVARCHAR";
			public const string NClob = "NCLOB";
			
			private readonly static Dictionary<string, string> OleDbTypesToDB2 = new(StringComparer.OrdinalIgnoreCase)
			{
				{ "DBTYPE_I2", SmallInt },
				{ "DBTYPE_I4", Integer },
				{ "DBTYPE_I8", BigInt },
				{ "DBTYPE_R4", Real },
				{ "DBTYPE_R8", Double },
				{ "DBTYPE_NUMERIC", Decimal },
				{ "DBTYPE_DBDATE", Date },
				{ "DBTYPE_DBTIME", Time },
				{ "DBTYPE_DBTIMESTAMP", TimeStamp },
				{ "DBTYPE_CHAR", Char },
				{ "DBTYPE_VARCHAR", VarChar },
				{ "DBTYPE_LONGVARCHAR", Clob },
				{ "DBTYPE_WCHAR", Graphic },
				{ "DBTYPE_WVARCHAR", VarGraphic },
				{ "DBTYPE_WLONGVARCHAR", DbClob },
				{ "DBTYPE_BINARY", Binary },
				{ "DBTYPE_VARBINARY", VarBinary },
				{ "DBTYPE_LONGVARBINARY", Blob },
			};

			internal static string FromOleDbType(string oleDbType)
				=> OleDbTypesToDB2.TryGetValue(oleDbType, out var dbType) ? dbType : oleDbType;

			internal static class Groups
			{
				public static readonly ReadOnlyCollection<string> StringTypes = new([
					Char, VarChar, Clob,
					Graphic, VarGraphic, DbClob,
					NChar,  NVarChar, NClob,
				]);

				public static readonly ReadOnlyCollection<string> VariableLengthStringTypes = new([
					VarChar, Clob,
					VarGraphic, DbClob,
					NVarChar, NClob,
				]);

				public static readonly ReadOnlyCollection<string> FixedLengthStringTypes = new([
					Char, Graphic, NChar,
				]);
			}
		}

		public static class SQL
		{
			public static string Delimiter(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.Sql)
				=> naming == DB2iSeriesNamingConvention.Sql ? "." : "/";

			public static string DummyTableName(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.Sql)
				=> naming == DB2iSeriesNamingConvention.Sql ?
					"SYSIBM.SYSDUMMY1" : "SYSIBM/SYSDUMMY1";

			public const string LastInsertedIdentityGetter = "IDENTITY_VAL_LOCAL()";
		}
	}
}
