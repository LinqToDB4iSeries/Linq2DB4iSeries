using System;
using System.Data.Common;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class ReaderExpressionTools
	{
		public static readonly Expression<Func<DbDataReader, int, string>> GetTrimmedStringExpression
				= (r, i) => TrimString(r.GetString(i));

		public static readonly Expression<Func<DbDataReader, int, string>> GetTrimmedCharExpression
				= (r, i) => TrimString(GetCharFromString(r.GetString(i)));

		public static string TrimString(string value)
			=> value?.TrimEnd(' ')!;

		private static string GetCharFromString(string str)
		{
			if (str.Length > 0)
				return str[0].ToString();

			return string.Empty;
		}
	}
}
