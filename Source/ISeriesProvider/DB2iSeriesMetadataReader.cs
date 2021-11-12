using System;
using System.Reflection;
using LinqToDB.Metadata;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using SqlQuery;
	using System.Collections.Generic;
	using System.Linq;

	class DB2iSeriesMetadataReader : IMetadataReader
	{
		private readonly string providerName;

		public DB2iSeriesMetadataReader(string providerName)
		{
			this.providerName = providerName;
		}

		public T[] GetAttributes<T>(Type type, MemberInfo memberInfo, bool inherit = true) where T : Attribute
		{
			if (typeof(Sql.ExpressionAttribute).IsAssignableFrom(typeof(T)))
			{
				switch (memberInfo.Name)
				{
					case "CharIndex":
						return GetFunction<T>(() => new Sql.FunctionAttribute("Locate"));

					case "Trim":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase))
						{
							return GetExpression<T>(() => new Sql.ExpressionAttribute(providerName, "Strip({0}, B, {1})"));
						}
						break;
					case "TrimLeft":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase) ||
							memberInfo.ToString().EndsWith("System.Nullable`1[System.Char])", StringComparison.CurrentCultureIgnoreCase))
						{
							return GetExpression<T>(() => new Sql.ExpressionAttribute(providerName, "Strip({0}, L, {1})"));
						}
						break;
					case "TrimRight":
						if (memberInfo.ToString().EndsWith("(Char[])", StringComparison.CurrentCultureIgnoreCase) ||
							memberInfo.ToString().EndsWith("System.Nullable`1[System.Char])", StringComparison.CurrentCultureIgnoreCase))
						{
							return GetExpression<T>(() => new Sql.ExpressionAttribute(providerName, "Strip({0}, T, {1})"));
						}
						break;
					case "Truncate":
						if (type == typeof(LinqExtensions)) //Do not handle TRUNCATE TABLE statement
							break;

						return typeof(T) == typeof(Sql.ExtensionAttribute) ?
							new[] { (T)(object)new Sql.ExtensionAttribute(providerName, "Truncate({0}, 0)") } :
							new[] { (T)(object)new Sql.ExpressionAttribute(providerName, "Truncate({0}, 0)") };

					case "DateAdd":
						return GetExtension<T>(() => new Sql.ExtensionAttribute(providerName, "") { ServerSideOnly = false, PreferServerSide = false, BuilderType = typeof(DateAddBuilderDB2i) });
					case "DatePart":
						return GetExtension<T>(() => new Sql.ExtensionAttribute(providerName, "") { ServerSideOnly = false, PreferServerSide = false, BuilderType = typeof(DatePartBuilderDB2i) });
					case "DateDiff":
						return GetExtension<T>(() => new Sql.ExtensionAttribute(providerName, "") { BuilderType = typeof(DateDiffBuilderDB2i) });
					case "TinyInt":
						return GetExpression<T>(() => new Sql.ExpressionAttribute(providerName, "SmallInt") { ServerSideOnly = true });
					case "Substring":
						return GetFunction<T>(() => new Sql.FunctionAttribute(providerName, "Substr") { PreferServerSide = true });
					case "Atan2":
						return GetFunction<T>(() => new Sql.FunctionAttribute(providerName, "Atan2", 1, 0));
					case "Log" when memberInfo is MethodInfo logMethod:
						if (logMethod.GetParameters().Length == 1)
							return GetFunction<T>(() => new Sql.FunctionAttribute(providerName, "Ln"));
						break;
					case "Log10":
						return GetFunction<T>(() => new Sql.FunctionAttribute(providerName, "Log10"));
					case "DefaultNChar":
					case "DefaultNVarChar":
					case "NChar":
					case "NVarChar":
						return GetFunction<T>(() => new Sql.FunctionAttribute(providerName, "Graphic") { ServerSideOnly = true });
					case "Replicate":
						return GetFunction<T>(() => new Sql.FunctionAttribute(providerName, "Repeat"));
					case "StringAggregate" when memberInfo is MethodInfo stringAggregateMethod:
						var firstParameter = stringAggregateMethod.GetParameters().Any(x => x.Name == "selector") ? "selector" : "source";
						return GetExtension<T>(() => new Sql.ExtensionAttribute(providerName, "LISTAGG({" + firstParameter + "}, {separator}){_}{aggregation_ordering?}") { IsAggregate = true, ChainPrecedence = 10 });
				}
			}

			return new T[] { };
		}

		private T[] GetExpression<T>(Func<Sql.ExpressionAttribute> build)
		{
			if (typeof(T) == typeof(Sql.ExpressionAttribute))
				return new[] { (T)(object)build() };
			else
				return new T[] { };
		}

		private T[] GetExtension<T>(Func<Sql.ExpressionAttribute> build)
		{
			if (typeof(T) == typeof(Sql.ExpressionAttribute) || typeof(T) == typeof(Sql.ExtensionAttribute))
				return new[] { (T)(object)build() };
			else
				return new T[] { };
		}

		private T[] GetFunction<T>(Func<Sql.ExpressionAttribute> build)
		{
			if (typeof(T) == typeof(Sql.ExpressionAttribute) || typeof(T) == typeof(Sql.FunctionAttribute))
				return new[] { (T)(object)build() };
			else
				return new T[] { };
		}

		public MemberInfo[] GetDynamicColumns(Type type)
		{
			return new MemberInfo[] { };
		}

		public T[] GetAttributes<T>(Type type, bool inherit = true) where T : Attribute
		{
			return new T[] { };
		}
	}

	public class DateAddBuilderDB2i : Sql.IExtensionCallBuilder
	{
		public void Build(Sql.ISqExtensionBuilder builder)
		{
			var part = builder.GetValue<Sql.DateParts>("part");
			var date = builder.GetExpression("date");
			var number = builder.GetExpression("number");

			string expStr;

			switch (part)
			{
				case Sql.DateParts.Year: expStr = "{0} + ({1}) Year"; break;
				case Sql.DateParts.Quarter: expStr = "{0} + (({1}) * 3) Month"; break;
				case Sql.DateParts.Month: expStr = "{0} + ({1}) Month"; break;
				case Sql.DateParts.DayOfYear:
				case Sql.DateParts.WeekDay:
				case Sql.DateParts.Day: expStr = "{0} + ({1}) Day"; break;
				case Sql.DateParts.Week: expStr = "{0} + (({1}) * 7) Day"; break;
				case Sql.DateParts.Hour: expStr = "{0} + ({1}) Hour"; break;
				case Sql.DateParts.Minute: expStr = "{0} + ({1}) Minute"; break;
				case Sql.DateParts.Second: expStr = "{0} + ({1}) Second"; break;
				case Sql.DateParts.Millisecond: expStr = "{0} + (({1}) * 1000) Microsecond"; break;
				default:
					throw new ArgumentOutOfRangeException();
			}

			builder.ResultExpression = new SqlExpression(typeof(DateTime?), expStr, Precedence.Additive, date, number);
		}
	}

	public class DatePartBuilderDB2i : Sql.IExtensionCallBuilder
	{
		public void Build(Sql.ISqExtensionBuilder builder)
		{
			string partStr;
			var part = builder.GetValue<Sql.DateParts>("part");
			switch (part)
			{
				case Sql.DateParts.Year: partStr = "YEAR({date})"; break;
				case Sql.DateParts.Quarter: partStr = "QUARTER({date})"; break;
				case Sql.DateParts.Month: partStr = "MONTH({date})"; break;
				case Sql.DateParts.DayOfYear: partStr = "DAYOFYEAR({date})"; break;
				case Sql.DateParts.Day: partStr = "DAY({date})"; break;
				case Sql.DateParts.Week: partStr = "WEEK({date})"; break;
				case Sql.DateParts.WeekDay: partStr = "DAYOFWEEK({date})"; break;
				case Sql.DateParts.Hour: partStr = "HOUR({date})"; break;
				case Sql.DateParts.Minute: partStr = "MINUTE({date})"; break;
				case Sql.DateParts.Second: partStr = "SECOND({date})"; break;
				case Sql.DateParts.Millisecond: partStr = "MICROSECOND({date}) / 1000"; break;
				default:
					throw new ArgumentOutOfRangeException();
			}

			builder.Expression = partStr;
		}
	}

	public class DateDiffBuilderDB2i : Sql.IExtensionCallBuilder
	{
		public void Build(Sql.ISqExtensionBuilder builder)
		{
			// Generally, this is used to wrap literal numbers as longs and doubles.
			// This is because the iSeries expects that literals are 32-bit integers.
			// This causes overflows unless the values are specifically cast to larger types.
			static SqlValue AsT<T>(T value)
			{
				return new SqlValue(value);
			}

			static ISqlExpression Days(ISqlExpression value)
			{
				return new SqlFunction(typeof(long), "DAYS", value);
			}

			static ISqlExpression MidnightSeconds(ISqlExpression value)
			{
				return new SqlFunction(typeof(long), "MIDNIGHT_SECONDS", value);
			}

			static ISqlExpression Microsecond(ISqlExpression value)
			{
				return new SqlFunction(typeof(long), "MICROSECOND", value);
			}

			// Takes a expression that may be a LAG or LEAD function.
			// If they are, the wrapper function needs to be applied within the LAG or LEAD function.
			// Otherwise, the value can be wrapped in the wrapper function directly.
			static ISqlExpression TransformLagAndLead(ISqlExpression iexpr, Func<ISqlExpression, ISqlExpression> wrapper)
			{
				if (iexpr is SqlExpression expr
					&& (expr.Expr.StartsWith("LAG", StringComparison.OrdinalIgnoreCase)
					|| expr.Expr.StartsWith("LEAD", StringComparison.OrdinalIgnoreCase)))
				{
					var newParams = new List<ISqlExpression>(expr.Parameters.Length);

					var param = expr.Parameters.First();
					// Wrap the parameter rather than the whole expression.
					newParams.Add(wrapper(param));
					newParams.AddRange(expr.Parameters.Skip(1));

					return new SqlExpression(expr.SystemType, expr.Expr, expr.Precedence, newParams.ToArray());
				}

				// Wrap the whole expression.
				return wrapper(iexpr);
			}

			var part = builder.GetValue<Sql.DateParts>(0);
			var startDate = builder.GetExpression(1);
			var endDate = builder.GetExpression(2);

			// If start or endDate are functions rather than values, they may need to be transformed.
			var startDays = TransformLagAndLead(startDate, Days);
			var endDays = TransformLagAndLead(endDate, Days);
			var startMidnightSeconds = TransformLagAndLead(startDate, MidnightSeconds);
			var endMidnightSeconds = TransformLagAndLead(endDate, MidnightSeconds);

			var secondsExpr = builder.Mul<long>(
				builder.Sub<long>(endDays, startDays),
				AsT(86400L));

			var midnight = builder.Sub<long>(endMidnightSeconds, startMidnightSeconds);

			var resultExpr = builder.Add<long>(secondsExpr, midnight);

			switch (part)
			{
				case Sql.DateParts.Day: resultExpr = builder.Div<long>(resultExpr, AsT(86400L)); break;
				case Sql.DateParts.Hour: resultExpr = builder.Div<long>(resultExpr, AsT(3600L)); break;
				case Sql.DateParts.Minute: resultExpr = builder.Div<long>(resultExpr, AsT(60L)); break;
				case Sql.DateParts.Second: break;
				case Sql.DateParts.Millisecond:
					var startMicrosecond = TransformLagAndLead(startDate, Microsecond);
					var endMicrosecond = TransformLagAndLead(endDate, Microsecond);
					resultExpr = builder.Add<long>(
						builder.Mul<long>(resultExpr, AsT(1000L)),
						builder.Div<long>(
							builder.Sub<long>(endMicrosecond, startMicrosecond),
							AsT(1000L)));
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}

			builder.ResultExpression = resultExpr;
		}
	}
}
