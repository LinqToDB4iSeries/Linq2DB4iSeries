using System;
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
        public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
        {
            if (attributes == null)
            {
                return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false);
            }

            var attribs = attributes.ToList();

            var mapGuidAsString = false;

            var attrib = attribs.FirstOrDefault(_ => _.Name == "MapGuidAsString");

            if (attrib != null)
            {
                bool.TryParse(attrib.Value, out mapGuidAsString);
            }

            var version = attribs.FirstOrDefault(_ => _.Name == "MinVer");
            if (version != null && version.Value == "7.1.38")
            {
                if (mapGuidAsString)
                {
                    return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73_GAS, DB2iSeriesLevels.V7_1_38, true);
                }

                return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73, DB2iSeriesLevels.V7_1_38, false);
            }

            if (mapGuidAsString)
            {
                return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_GAS, DB2iSeriesLevels.Any, true);
            }

            return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false);
        }
    }
}