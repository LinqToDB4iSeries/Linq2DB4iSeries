using System;
using System.Collections.Concurrent;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class DB2iSeriesLoadExpressions
	{
		static readonly ConcurrentDictionary<string, object> configurations = new();

		public static void SetupExpressions(string configuration, bool mapGuidAsString)
		{
			if (configurations.TryAdd(configuration, null))
			{
				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.Space(0)).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L<int?, string>(p0 => Linq.Expressions.VarChar(Linq.Expressions.Replicate(" ", p0), 1000))));
				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.Stuff("", 0, 0, "")).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L<string, int?, int?, string, string>((p0, p1, p2, p3) => Linq.Expressions.AltStuff(p0, p1, p2, p3))));
				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.PadRight("", 0, ' ')).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L<string, int?, char?, string>((p0, p1, p2) => p0.Length > p1 ? p0 : p0 + Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000))));
				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.PadLeft("", 0, ' ')).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L<string, int?, char?, string>((p0, p1, p2) => p0.Length > p1 ? p0 : Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000) + p0)));
				Linq.Expressions.MapMember(
					configuration,
					Linq.Expressions.M(() => Sql.ConvertTo<string>.From((decimal)0)).MemberInfo,
					Linq.Expressions.N(() => Linq.Expressions.L((decimal p) => Sql.TrimLeft(Sql.Convert<string, decimal>(p), '0'))));

				if (!mapGuidAsString)
				{
					Linq.Expressions.MapMember(
						configuration,
						Linq.Expressions.M(() => Sql.ConvertTo<string>.From(Guid.Empty)).MemberInfo,
						Linq.Expressions.N(() => Linq.Expressions.L(
							(Guid p) => Sql.Lower(Sql.Substring(Linq.Expressions.Hex(p), 7, 2)
												  + Sql.Substring(Linq.Expressions.Hex(p), 5, 2)
												  + Sql.Substring(Linq.Expressions.Hex(p), 3, 2)
												  + Sql.Substring(Linq.Expressions.Hex(p), 1, 2)
												  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 11, 2)
												  + Sql.Substring(Linq.Expressions.Hex(p), 9, 2)
												  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 15, 2)
												  + Sql.Substring(Linq.Expressions.Hex(p), 13, 2)
												  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 17, 4)
												  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 21, 12)))));
				}

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
