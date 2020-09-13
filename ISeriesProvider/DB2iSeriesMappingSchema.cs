using System;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Mapping;
	using SqlQuery;

	public class DB2iSeriesMappingSchema : MappingSchema
	{
		public DB2iSeriesMappingSchema() : this(DB2iSeriesProviderName.DB2)
		{
		}

		public DB2iSeriesMappingSchema(string configuration, params MappingSchema[] schemas) : base(configuration, schemas)
		{
			if (configuration != DB2iSeriesProviderName.DB2_GAS)
			{
				SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => ConvertGuidToSql(sb, (Guid)v));
			}

			ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

			SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));
			SetValueToSqlConverter(typeof(string), (sb, dt, v) => ConvertStringToSql(sb, v.ToString()));
			SetValueToSqlConverter(typeof(char), (sb, dt, v) => ConvertCharToSql(sb, (char)v));
			SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => ConvertDateTimeToSql(sb, dt, (DateTime)v));

			AddMetadataReader(new DB2iSeriesMetadataReader(configuration));
#if !NETSTANDARD2_0
			AddMetadataReader(new DB2iSeriesAttributeReader());
#endif
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
			var format = datatype.Type.DataType switch
			{
				DataType.Date => "'{0:yyyy-MM-dd}'",

				DataType.Time => value.Millisecond == 0 ?
									"'{0:HH:mm:ss}'" :
									"'{0:HH:mm:ss.fff}'",

				_ => value.Millisecond == 0 ?
						"'{0:yyyy-MM-dd HH:mm:ss}'" :
						"'{0:yyyy-MM-dd HH:mm:ss.fff}'"
			};

			stringBuilder.AppendFormat(format, value);
		}

		private static void ConvertGuidToSql(StringBuilder stringBuilder, Guid value)
		{
			var s = value.ToString("N");
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
