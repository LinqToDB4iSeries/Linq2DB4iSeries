using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Reflection;
using LinqToDB.Configuration;
using LinqToDB.Linq;
using System.Linq;
using LinqToDB.Data;

namespace LinqToDB.DataProvider.DB2iSeries
{
    public class DB2iSeriesFactory : IDataProviderFactory
	{
		public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
		{
			if (attributes == null)
                return DB2iSeriesTools.GetDataProvider(DB2iSeriesProviderName.DB2);

            var options = DB2iSeriesDataProviderOptions.FromAttributes(attributes);
            var providerName = DB2iSeriesProviderName.GetFromOptions(options);

            if (providerName != null && DB2iSeriesTools.TryGetDataProvider(providerName, out var dataProvider))
                return dataProvider;
            
            else return new DB2iSeriesDataProvider(options);
        }
	}
}