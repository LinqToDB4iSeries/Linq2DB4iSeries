using LinqToDB.SqlQuery;
using System.Reflection;

namespace Tests
{
	public static class StatementShims
	{
		public static void PrepareQueryAndAliases(this SqlSelectStatement statement)
		{
			typeof(SqlStatement).GetMethod(nameof(PrepareQueryAndAliases), BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(statement, new object[] { });
		}
	}
}
