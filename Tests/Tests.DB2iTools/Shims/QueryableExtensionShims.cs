using LinqToDB.Linq;
using LinqToDB.SqlQuery;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Tests
{
	public static class QueryableExtensionShims
	{ 
		public static SqlStatement GetStatement<T>(this IQueryable<T> query)
		{
			var eq = (IExpressionQuery)query;
			var expression = eq.Expression;
			var info = Query<T>.GetQuery(eq.DataContext, ref expression);

			InitParameters(eq, info, expression);

			var queries = (IEnumerable<object>)info.GetType().GetField("Queries", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(info);
			var queryInfo = queries.First();
			return (SqlStatement)queryInfo.GetType().GetProperty("Statement", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(queryInfo);
		}

		private static void InitParameters(IExpressionQuery eq, Query info, Expression expression)
		{
			eq.DataContext.GetQueryRunner(info, 0, expression, null, null).GetSqlText();
		}
	}
}
