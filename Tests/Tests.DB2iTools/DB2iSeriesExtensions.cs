using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Linq;
using LinqToDB.SqlQuery;
using LinqToDB.SchemaProvider;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

#nullable disable

#if NET472
namespace System.Diagnostics.CodeAnalysis
{
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter | AttributeTargets.ReturnValue, AllowMultiple = true)]
	public sealed class NotNullAttribute : Attribute
	{
		public string ParameterName { get; }

		public NotNullAttribute()
		{

		}

		public NotNullAttribute(string parameterName)
		{
			ParameterName = parameterName;
		}
	}
}
#endif

namespace IBM.Data.Informix 
{
	public class IfxTimeSpan { }
}

namespace Tests
{
	public static class Query<T>
	{
		private static readonly Lazy<Type> query = new Lazy<Type>(() => typeof(Query).Assembly.GetTypes()
				.First(x => x.Name.StartsWith(nameof(Query) + "`")
				&& x.IsGenericTypeDefinition
				&& x.GetGenericArguments().Count() == 1)
				.MakeGenericType(typeof(T)));
		
		public static Query GetQuery(IDataContext dataContext, ref Expression expression)
			=> (Query)query.Value
				.GetMethod(nameof(GetQuery), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(null, new object[] { dataContext, expression });
		
		public static void ClearCache()
			=> query.Value
				.GetMethod(nameof(ClearCache), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(null, new object[] { });
		
		public static int CacheMissCount
			=> (int)query.Value
				.GetProperty(nameof(CacheMissCount))
				.GetValue(null);
	}

	public static class QueryableExtensions
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

	public static class DataContextExtensions
	{
		public static DataConnection GetDataConnection(this DataContext dataContext)
		{
			return (DataConnection)dataContext.GetType()
				.GetMethod(nameof(GetDataConnection), BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
				.Invoke(dataContext, new object[] { });
		}
	}

	public static class SchemaProviderBaseExtensions
	{
		public static void SetForeignKeyMemberName(GetSchemaOptions getSchemaOptions, TableSchema tableSchema, ForeignKeySchema foreignKeySchema)
		{
			typeof(LinqToDB.SchemaProvider.SchemaProviderBase).GetMethod(nameof(SetForeignKeyMemberName), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(null, new object[] { getSchemaOptions, tableSchema, foreignKeySchema });
		}
	}

	public static class StatementExtensions
	{
		public static void PrepareQueryAndAliases(this SqlSelectStatement statement)
		{
			typeof(SqlStatement).GetMethod(nameof(PrepareQueryAndAliases), BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(statement, new object[] { });
		}
	}
}
