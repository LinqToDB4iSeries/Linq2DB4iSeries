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
		public DB2iSeriesMappingSchema() : this(DB2iSeriesFactory.ProviderName)
		{
		}

		static internal readonly DB2iSeriesMappingSchema Instance = new DB2iSeriesMappingSchema();
		protected DB2iSeriesMappingSchema(string configuration) : base(configuration)
		{
			SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => ConvertGuidToSql(sb, (Guid)v));
			SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));
			SetValueToSqlConverter(typeof(string), (sb, dt, v) => ConvertStringToSql(sb, v.ToString()));
			SetValueToSqlConverter(typeof(char), (sb, dt, v) => ConvertCharToSql(sb, (char)v));
			SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => ConvertDateTimeToSql(sb, dt, (DateTime)v));
		}

		public static string GetiSeriesType(SqlDataType dataType)
		{
			switch (dataType.DataType)
			{
				case DataType.Variant:
				case DataType.Binary:
					return $"BINARY({(dataType.Length == 0 ? 1 : dataType.Length)})";
				case DataType.Int64:
					return "BIGINT";
				case DataType.Blob:
					return $"BLOB({ (dataType.Length == 0 ? 1 : dataType.Length)})";
				case DataType.VarBinary:
					return $"VARBINARY({ (dataType.Length == 0 ? 1 : dataType.Length)})";
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
					return $"VARCHAR({ (dataType.Length == 0 ? 1 : dataType.Length)})";
				case DataType.NVarChar:
					return $"NVARCHAR({ (dataType.Length == 0 ? 1 : dataType.Length)})";
				default:
					return dataType.DataType.ToString();
			}
		}

		public override T GetAttribute<T>(MemberInfo memberInfo, Func<T, string> configGetter, bool inherit = true)
		{
			var specific = GetSpecificAttributes<T>(memberInfo);
			if (specific != default(T))
				return specific;

			return base.GetAttribute<T>(memberInfo, configGetter, inherit);
		}


		protected T GetSpecificAttributes<T>(MemberInfo memberInfo)
		{
			if (typeof(T) == typeof(Sql.ExpressionAttribute))
			{
				switch (memberInfo.Name)
				{
					case "CharIndex":
						return (T)(object)new Sql.FunctionAttribute("Locate");

					case "Trim":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase))
						{
							return (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Strip({0}, B, {1})");
						}
						break;
					case "TrimLeft":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase) ||
							memberInfo.ToString().EndsWith("System.Nullable`1[System.Char])", StringComparison.CurrentCultureIgnoreCase))
						{
							return (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Strip({0}, L, {1})");
						}
						break;
					case "TrimRight":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase) ||
							memberInfo.ToString().EndsWith("System.Nullable`1[System.Char])", StringComparison.CurrentCultureIgnoreCase))
						{
							return (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Strip({0}, T, {1})");
						}
						break;
					case "Truncate":
						return (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Truncate({0}, 0)");
					case "DateAdd":
						return (T)(object)new Sql.DatePartAttribute(DB2iSeriesFactory.ProviderName, "{{1}} + {0}", Precedence.Additive, true, new[] { "{0} Year", "({0} * 3) Month", "{0} Month", "{0} Day", "{0} Day", "({0} * 7) Day", "{0} Day", "{0} Hour", "{0} Minute", "{0} Second", "({0} * 1000) Microsecond" }, 0, 1, 2);
					case "DatePart":
						return (T)(object)new Sql.DatePartAttribute(DB2iSeriesFactory.ProviderName, "{0}", false, new[] { null, null, null, null, null, null, "DayOfWeek", null, null, null, null }, 0, 1);
					case "TinyInt":
						return (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "SmallInt") { ServerSideOnly = true };
					case "DefaultNChar":
					case "DefaultNVarChar":
						return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Char") { ServerSideOnly = true };
					case "Substring":
						return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Substr") { PreferServerSide = true };
					case "Atan2":
						return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Atan2", 1, 0);
					case "Log":
						return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Ln");
					case "Log10":
						return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Log");
					case "NChar":
					case "NVarChar":
						return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Char") { ServerSideOnly = true };
					case "Replicate":
						return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Repeat");
				}
			}

			return default(T);
		}


		private static void AppendConversion(StringBuilder stringBuilder, int value)
		{
			stringBuilder.Append("varchar(").Append(value).Append(")");
		}

		private static void ConvertStringToSql(StringBuilder stringBuilder, string value)
		{
			DataTools.ConvertStringToSql(stringBuilder, "||", "'", AppendConversion, value);
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
