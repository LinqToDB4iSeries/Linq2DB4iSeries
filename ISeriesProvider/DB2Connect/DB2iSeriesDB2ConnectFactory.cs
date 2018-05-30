using System.Linq;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Reflection;
using LinqToDB.Configuration;
using LinqToDB.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{

	public class DB2iSeriesDB2ConnectFactory : IDataProviderFactory
	{
		public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
        {
            if (attributes == null)
            {
                return new DB2iSeriesDataProvider(DB2iSeriesDB2ConnectProviderName.DB2, DB2iSeriesLevels.Any, false);
            }

            var attribs = attributes.ToList();

            var mapGuidAsString = false;

            var attrib = attribs.FirstOrDefault(_ => _.Name == DB2iSeriesTools.MapGuidAsString);

            if (attrib != null)
            {
                bool.TryParse(attrib.Value, out mapGuidAsString);
            }

            var version = attribs.FirstOrDefault(_ => _.Name == "MinVer");
            var level = version != null && version.Value == "7.1.38" ? DB2iSeriesLevels.V7_1_38 : DB2iSeriesLevels.Any;

            if (mapGuidAsString)
            {
                return new DB2iSeriesDB2ConnectDataProvider(DB2iSeriesDB2ConnectProviderName.DB2_GAS, level, true);
            }

            return new DB2iSeriesDB2ConnectDataProvider(DB2iSeriesDB2ConnectProviderName.DB2, level, false);
        }
    }
}