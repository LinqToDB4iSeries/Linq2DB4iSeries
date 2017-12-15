using System.Collections.Generic;
using System.Collections.Specialized;
using System.Reflection;
using LinqToDB.Configuration;
using LinqToDB.Linq;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{

	public class DB2iSeriesFactory : IDataProviderFactory
	{
		public static string ProviderName = "DB2.iSeries";

		public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
		{
		    if (attributes != null)
		    {
		        var version = attributes.FirstOrDefault(_ => _.Name == "MinVer");
		        if (version != null)
		        {
		            switch (version.Value)
		            {
		                case "7.1.38":
		                    return new DB2iSeriesDataProvider(DB2iSeriesLevels.V7_1_38);
		            }
		        }
		    }

		    return new DB2iSeriesDataProvider(DB2iSeriesLevels.Any);
		}
	}
}