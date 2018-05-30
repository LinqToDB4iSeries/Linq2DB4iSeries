using System.Collections.Generic;
using System.Collections.Specialized;
using System.Reflection;
using LinqToDB.Configuration;
using LinqToDB.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{

	public class DB2iSeriesDB2ConnectFactory : IDataProviderFactory
	{
		public static string ProviderName = "DB2.iSeriesDB2Connect";

		public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
		{
			DB2iSeriesExpressions.LoadExpressions(ProviderName);

			return new DB2iSeriesDB2ConnectDataProvider();
		}
	}
}