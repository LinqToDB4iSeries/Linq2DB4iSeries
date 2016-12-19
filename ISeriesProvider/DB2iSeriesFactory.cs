using System.Collections.Generic;
using System.Collections.Specialized;
using System.Reflection;
using LinqToDB.Configuration;
using LinqToDB.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{

	public class DB2iSeriesFactory : IDataProviderFactory
	{
		public static string ProviderName = "DB2.iSeries";

		public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
		{
			DB2iSeriesExpressions.LoadExpressions();

			return new DB2iSeriesDataProvider();
		}
	}
}