using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using LinqToDB.Metadata;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using SqlQuery;

	class DB2iSeriesMetadataReader : IMetadataReader
	{
		public T[] GetAttributes<T>(MemberInfo memberInfo, bool inherit = true) where T : Attribute
		{
			if (typeof(T) == typeof(Sql.ExpressionAttribute))
			{
				switch (memberInfo.Name)
				{
					case "CharIndex":
						return new[] { (T)(object)new Sql.FunctionAttribute("Locate") };

					case "Trim":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase))
						{
							return new[] { (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Strip({0}, B, {1})") };
						}
						break;
					case "TrimLeft":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase) ||
							memberInfo.ToString().EndsWith("System.Nullable`1[System.Char])", StringComparison.CurrentCultureIgnoreCase))
						{
							return new[] { (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Strip({0}, L, {1})") };
						}
						break;
					case "TrimRight":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase) ||
							memberInfo.ToString().EndsWith("System.Nullable`1[System.Char])", StringComparison.CurrentCultureIgnoreCase))
						{
							return new[] { (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Strip({0}, T, {1})") };
						}
						break;
					case "Truncate":
						return new[] { (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "Truncate({0}, 0)") };
					case "DateAdd":
						return new[] { (T)(object)new Sql.DatePartAttribute(DB2iSeriesFactory.ProviderName, "{{1}} + {0}", Precedence.Additive, true, new[] { "{0} Year", "({0} * 3) Month", "{0} Month", "{0} Day", "{0} Day", "({0} * 7) Day", "{0} Day", "{0} Hour", "{0} Minute", "{0} Second", "({0} * 1000) Microsecond" }, 0, 1, 2) };
					case "DatePart":
						return new[] { (T)(object)new Sql.DatePartAttribute(DB2iSeriesFactory.ProviderName, "{0}", false, new[] { null, null, null, null, null, null, "DayOfWeek", null, null, null, null }, 0, 1) };
					case "TinyInt":
						return new[] { (T)(object)new Sql.ExpressionAttribute(DB2iSeriesFactory.ProviderName, "SmallInt") { ServerSideOnly = true } };
					case "DefaultNChar":
					case "DefaultNVarChar":
						return new[] { (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Char") { ServerSideOnly = true } };
					case "Substring":
						return new[] { (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Substr") { PreferServerSide = true } };
					case "Atan2":
						return new[] { (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Atan2", 1, 0) };
					case "Log":
						return new[] { (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Ln") };
					case "Log10":
						return new[] { (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Log") };
					case "NChar":
					case "NVarChar":
						return new[] { (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Char") { ServerSideOnly = true } };
					case "Replicate":
						return new[] { (T)(object)new Sql.FunctionAttribute(DB2iSeriesFactory.ProviderName, "Repeat") };
				}
			}

			return new T[] { };
		}

		public T[] GetAttributes<T>(Type type, bool inherit = true) where T : Attribute
		{
			return new T[] { };
		}
	}
}
