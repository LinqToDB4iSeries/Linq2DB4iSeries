using System;
using System.Reflection;
using System.Linq;

using static LinqToDB.Sql;

using LinqToDB.SqlQuery;
using LinqToDB.Internal.SqlQuery;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class TrimBuilderDB2i : IExtensionCallBuilder
	{
		public void Build(Sql.ISqlExtensionBuilder builder)
		{
			var stringExpression = builder.GetExpression(0);
			
			char[]? chars = null;
			if (builder.Member is not MethodInfo methodInfo)
				throw new InvalidOperationException("Member is not a trim method.");

			if (methodInfo.GetParameters().Length == 2)
			{
				var charParameter = methodInfo.GetParameters().Last();

				if (charParameter.ParameterType == typeof(char[]))
				{
					chars = builder.GetValue<char[]>(1).Distinct().ToArray();
				}
				else if (charParameter.ParameterType == typeof(char?[]))
				{
					chars = builder.GetValue<char?[]>(1).Where(x => x.HasValue).Select(x => x!.Value).Distinct().ToArray();
				}
				else if (charParameter.ParameterType == typeof(char))
				{
					chars = new[] { builder.GetValue<char>(1) };
				}
				else if (charParameter.ParameterType == typeof(char?))
				{
					chars = new[] { builder.GetValue<char?>(1) ?? ' ' };
				}
				else
					throw new InvalidOperationException("Argument is not char[] or char?[]");
			}

			var direction = builder.Member.Name switch
			{
				nameof(Linq.Expressions.TrimLeft) => "LTRIM",
				nameof(Linq.Expressions.TrimRight) => "RTRIM",
				_ => "TRIM",
			};

			if (chars == null || chars.Length == 0)
			{
				builder.ResultExpression = new SqlFunction(
					builder.Mapping.GetDbDataType(typeof(string)),
					direction,
					stringExpression!);
				return;
			}

			if (!builder.DataContext.SqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsTrimCharacters))
				throw new LinqToDBException("TrimLeft/TrimRight with multiple characters not supported on i series version 7.1");

			builder.ResultExpression = new SqlExpression(
				builder.Mapping.GetDbDataType(typeof(string)),
				direction + "({0}, {1})",
				Precedence.Primary,
				stringExpression!,
				new SqlExpression(builder.Mapping.GetDbDataType(typeof(string)), "{0}", new SqlValue(new string(chars))));
		}
	}
}
