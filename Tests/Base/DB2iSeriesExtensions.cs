using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Linq;
using LinqToDB.SqlQuery;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

#nullable disable

namespace Tests
{
	public static class Query<T>
	{
		public static Query GetQuery(IDataContext dataContext, ref Expression expression)
		{
			var m = typeof(Query).Assembly.GetTypes()
				.First(x => x.Name.StartsWith("Query`")
				&& x.IsGenericTypeDefinition
				&& x.GetGenericArguments().Count() == 1)
				.MakeGenericType(typeof(T))
				.GetMethod("GetQuery", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);


			return (Query)m.Invoke(null, new object[] { dataContext, expression });
		}
	}

	public static class QueryableExtensions
	{ 
		public static SqlStatement GetStatement<T>(this IQueryable<T> query)
		{
			var eq = (IExpressionQuery)query;
			var expression = eq.Expression;
			var info = Query<T>.GetQuery(eq.DataContext, ref expression);

			InitParameters(eq, info, expression);

			var queries = (IEnumerable<object>)info.GetType().GetField("Queries", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic).GetValue(info);
			var queryInfo = queries.First();
			return (SqlStatement)queryInfo.GetType().GetProperty("Statement", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic).GetValue(queryInfo);
		}

		private static void InitParameters(IExpressionQuery eq, Query info, Expression expression)
		{
			eq.DataContext.GetQueryRunner(info, 0, expression, null, null).GetSqlText();
		}
	}

	public static class DataContextExtensions
	{
		public static DataConnection GetDataConnection(this DataContext dataContext)
		{
			return (DataConnection)dataContext.GetType()
				.GetMethod(nameof(GetDataConnection), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public)
				.Invoke(dataContext, new object[] { });
		}
	}
}
