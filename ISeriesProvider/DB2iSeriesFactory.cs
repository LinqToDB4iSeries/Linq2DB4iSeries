using System.Collections.Generic;
using System.Linq;
using LinqToDB.Configuration;

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

			var attrib = attribs.FirstOrDefault(_ => _.Name == DB2iSeriesTools.MapGuidAsString);

			if (attrib != null)
			{
				bool.TryParse(attrib.Value, out mapGuidAsString);
			}

			var version = attribs.FirstOrDefault(_ => _.Name == "MinVer");
			var level = version != null && version.Value == "7.1.38" ? DB2iSeriesLevels.V7_1_38 : DB2iSeriesLevels.Any;

			if (mapGuidAsString)
			{
				return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_GAS, level, true);
			}

			return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, level, false);
		}
	}
}
