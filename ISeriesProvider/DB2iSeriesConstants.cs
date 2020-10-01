using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class Constants
	{
		public static class ProviderFlags
		{
			public const string MapGuidAsString = "MapGuidAsString";
			public const string MinimumVersion = "MinVer";
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
		}

		public static class SQL
		{
			public static string DummyTableName(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.Sql)
				=> naming == DB2iSeriesNamingConvention.System ?
					"SYSIBM/SYSDUMMY1" : "SYSIBM.SYSDUMMY1";

			public const string LastInsertedIdentityGetter = "IDENTITY_VAL_LOCAL()";
		}
	}
}
