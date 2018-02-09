﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using LinqToDB;
using LinqToDB.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class DB2iSeriesExpressions
	{
		public static void LoadExpressions(string providerName)
		{
			LinqToDB.Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Space(0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Int32?, String>(p0 => Sql.Convert(Sql.VarChar(1000), Linq.Expressions.Replicate(" ", p0)))));
			LinqToDB.Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Stuff("", 0, 0, "")),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Int32?, String, String>((p0, p1, p2, p3) => Linq.Expressions.AltStuff(p0, p1, p2, p3))));
			LinqToDB.Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.PadRight("", 0, ' ')),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Char?, String>((p0, p1, p2) => p0.Length > p1 ? p0 : p0 + Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000))));
			LinqToDB.Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.PadLeft("", 0, ' ')),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Char?, String>((p0, p1, p2) => p0.Length > p1 ? p0 : Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000) + p0)));
			LinqToDB.Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.ConvertTo<String>.From((Decimal)0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Decimal, String>((Decimal p) => Sql.TrimLeft(Sql.Convert<string, Decimal>(p), '0'))));

		    if (providerName == DB2iSeriesProviderName.DB2 || providerName == DB2iSeriesProviderName.DB2_73)
		    {
		        LinqToDB.Linq.Expressions.MapMember(
		            providerName,
		            Linq.Expressions.M(() => Sql.ConvertTo<String>.From(Guid.Empty)),
		            Linq.Expressions.N(() => Linq.Expressions.L<Guid, String>(
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

            LinqToDB.Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Log(0m, 0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Decimal?, Decimal?, Decimal?>((m, n) => Sql.Log(n) / Sql.Log(m))));
			LinqToDB.Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Log(0.0, 0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Double?, Double?, Double?>((m, n) => Sql.Log(n) / Sql.Log(m))));
		}
	}
}
