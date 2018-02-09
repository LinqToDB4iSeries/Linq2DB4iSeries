using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using System.Reflection;
	using Extensions;
	using Mapping;
	using SqlQuery;

	public class DB2iSeriesMappingSchema : MappingSchema
	{
	    private readonly bool mapGuidAsString;
	    public DB2iSeriesMappingSchema() : this(DB2iSeriesProviderName.DB2)
	    {
	    }

		// static internal readonly DB2iSeriesMappingSchema Instance = new DB2iSeriesMappingSchema();
		public DB2iSeriesMappingSchema(string configuration) : base(configuration)
		{
		    mapGuidAsString = configuration == DB2iSeriesProviderName.DB2_73_GAS || configuration == DB2iSeriesProviderName.DB2_GAS;

		    if (!mapGuidAsString)
		    {
		        SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => ConvertGuidToSql(sb, (Guid)v));
            }

		    SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));
			SetValueToSqlConverter(typeof(string), (sb, dt, v) => ConvertStringToSql(sb, v.ToString()));
			SetValueToSqlConverter(typeof(char), (sb, dt, v) => ConvertCharToSql(sb, (char)v));
			SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => ConvertDateTimeToSql(sb, dt, (DateTime)v));

			AddMetadataReader(new DB2iSeriesMetadataReader(configuration));
			AddMetadataReader(new DB2iSeriesAttributeReader());
		}

		public static string GetiSeriesType(SqlDataType dataType)
		{
			switch (dataType.DataType)
			{
				case DataType.Variant:
				case DataType.Binary:
					return string.Format("BINARY({0})", (dataType.Length == 0 ? 1 : dataType.Length));
				case DataType.Int64:
					return "BIGINT";
				case DataType.Blob:
					return string.Format("BLOB({0})", (dataType.Length == 0 ? 1 : dataType.Length));
				case DataType.VarBinary:
					return string.Format("VARBINARY({0})", (dataType.Length == 0 ? 1 : dataType.Length));
				case DataType.Char: return "CHAR";
				case DataType.Date: return "DATE";
				case DataType.Decimal: return "DECIMAL";
				case DataType.Double: return "DOUBLE";
				case DataType.Int32: return "INTEGER";
				case DataType.Single: return "REAL";
				case DataType.Int16:
				case DataType.Boolean:
					return "SMALLINT";
				case DataType.Time:
				case DataType.DateTimeOffset:
					return "TIME";
				case DataType.Timestamp:
				case DataType.DateTime:
				case DataType.DateTime2:
					return "TIMESTAMP";
				case DataType.VarChar:
					return string.Format("VARCHAR({0})", (dataType.Length == 0 ? 1 : dataType.Length));
				case DataType.NVarChar:
					return string.Format("NVARCHAR({0})", (dataType.Length == 0 ? 1 : dataType.Length));
				default:
					return dataType.DataType.ToString();
			}
		}


		private static void AppendConversion(StringBuilder stringBuilder, int value)
		{
			stringBuilder.Append("varchar(").Append(value).Append(")");
		}

		private static void ConvertStringToSql(StringBuilder stringBuilder, string value)
		{
			DataTools.ConvertStringToSql(stringBuilder, "||", "", AppendConversion, value, null);
		}

		private static void ConvertCharToSql(StringBuilder stringBuilder, char value)
		{
			DataTools.ConvertCharToSql(stringBuilder, "'", AppendConversion, value);
		}

		private static void ConvertDateTimeToSql(StringBuilder stringBuilder, SqlDataType datatype, DateTime value)
		{
			var format = value.Millisecond == 0 ?
						"'{0:yyyy-MM-dd HH:mm:ss}'":
						"'{0:yyyy-MM-dd HH:mm:ss.fff}'";

			if (datatype.DataType == DataType.Date)
				format = "'{0:yyyy-MM-dd}'";

			if (datatype.DataType == DataType.Time)
			{
				format = value.Millisecond == 0 ?
							"'{0:HH:mm:ss}'":
							"'{0:HH:mm:ss.fff}'";
			}

			stringBuilder.AppendFormat(format, value);
		}

		private static void ConvertGuidToSql(StringBuilder stringBuilder, Guid value)
		{
			dynamic s = value.ToString("N");
			stringBuilder
			  .Append("Cast(x'")
			  .Append(s.Substring(6, 2))
			  .Append(s.Substring(4, 2))
			  .Append(s.Substring(2, 2))
			  .Append(s.Substring(0, 2))
			  .Append(s.Substring(10, 2))
			  .Append(s.Substring(8, 2))
			  .Append(s.Substring(14, 2))
			  .Append(s.Substring(12, 2))
			  .Append(s.Substring(16, 16))
			  .Append("' as char(16) for bit data)");
		}
    }
}
