using System.Collections.Concurrent;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class DB2iSeriesLoadExpressions
	{
		static readonly ConcurrentDictionary<string, object?> configurations = new();

		public static void SetupExpressions(string configuration)
		{
			if (configurations.TryAdd(configuration, null))
			{
				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.Stuff("", 0, 0, "")).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L<string, int?, int?, string, string>((p0, p1, p2, p3) => Sql.Left(p0, p1 - 1) + p3 + Sql.Right(p0, p0.Length - (p1 + p2 - 1)))));

				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.Log(0m, 0)).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L<decimal?, decimal?, decimal?>((m, n) => Sql.Log(n) / Sql.Log(m))));

				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.Log(0.0, 0)).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L<double?, double?, double?>((m, n) => Sql.Log(n) / Sql.Log(m))));
			}
		}
	}
}
