using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class ReaderExpressionTools
	{
		public static readonly Expression<Func<IDataReader, int, string>> GetTrimmedStringExpression
				= (r, i) => TrimString(r.GetString(i));

		private static string TrimString(string value)
			=> value?.TrimEnd(' ');
	}
}
