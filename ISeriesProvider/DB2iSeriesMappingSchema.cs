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
			ValueToSqlConverter.ParameterValueExpression = (dataType, value) =>
			{
				string colType = "CHAR";

				if (dataType != null)
				{
					var actualType = SqlDataType.GetDataType(dataType.Type);
					switch (actualType.DataType)
					{
						case DataType.Variant:
						case DataType.Binary:
							colType = $"BINARY({(actualType.Length == 0 ? 1 : actualType.Length)})";
							break;
						case DataType.Int64:
							colType = "BIGINT";
							break;
						case DataType.Blob:
							colType = $"BLOB({ (actualType.Length == 0 ? 1 : actualType.Length)})";
							break;
						case DataType.VarBinary:
							colType = $"VARBINARY({ (actualType.Length == 0 ? 1 : actualType.Length)})";
							break;
						case DataType.Char: colType = "CHAR"; break;
						case DataType.Date: colType = "DATE"; break;
						case DataType.Decimal: colType = "DECIMAL"; break;
						case DataType.Double: colType = "DOUBLE"; break;
						case DataType.Int32: colType = "INTEGER"; break;
						case DataType.Single: colType = "REAL"; break;
						case DataType.Int16:
						case DataType.Boolean:
							colType = "SMALLINT";
							break;
						case DataType.Time:
						case DataType.DateTimeOffset:
							colType = "TIME";
							break;
						case DataType.Timestamp:
						case DataType.DateTime:
						case DataType.DateTime2:
							colType = "TIMESTAMP";
							break;
						case DataType.VarChar:
							colType = $"VARCHAR({ (actualType.Length == 0 ? 1 : actualType.Length)})";
							break;
						case DataType.NVarChar:
							colType = $"NVARCHAR({ (actualType.Length == 0 ? 1 : actualType.Length)})";
							break;
						default:
							colType = actualType.DataType.ToString();
							break;
					}
				}

				return $"CAST({value} AS {colType})";
			};
		}

		protected override T GetSpecificAttributes<T>(MemberInfo memberInfo)
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
			//else if (typeof(T) == typeof(Sql.FunctionAttribute))
			//{
			//	switch (memberInfo.Name)
			//	{
			//		case "Substring":
			//			return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Substr") { PreferServerSide = true };
			//		case "Atan2":
			//			return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Atan2", 1, 0);
			//		case "Log":
			//			return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Ln");
			//		case "Log10":
			//			return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Log");
			//		case "NChar":
			//		case "NVarChar":
			//			return (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Char") { ServerSideOnly = true };

			//	}
			//}
			//else if (typeof(T) == typeof(Sql.DatePartAttribute))
			//{
			//	switch (memberInfo.Name)
			//	{
			//		case "DateAdd":
			//			return (T)(object)new Sql.DatePartAttribute(DB2iSeriesFactory.ProviderName, "{{1}} + {0}", Precedence.Additive, true, new[] { "{0} Year", "({0} * 3) Month", "{0} Month", "{0} Day", "{0} Day", "({0} * 7) Day", "{0} Day", "{0} Hour", "{0} Minute", "{0} Second", "({0} * 1000) Microsecond" }, 0, 1, 2);
			//		case "DatePart":
			//			return (T)(object)new Sql.DatePartAttribute(DB2iSeriesFactory.ProviderName, "{0}", false, new[] { null, null, null, null, null, null, "DayOfWeek", null, null, null, null }, 0, 1);
			//	}
			//}


			return base.GetSpecificAttributes<T>(memberInfo);
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
