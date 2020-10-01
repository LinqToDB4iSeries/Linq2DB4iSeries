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

			var attrib = attribs.FirstOrDefault(_ => _.Name == Constants.ProviderFlags.MapGuidAsString);

			if (attrib != null)
			{
				bool.TryParse(attrib.Value, out mapGuidAsString);
			}

			var version = attribs.FirstOrDefault(_ => _.Name == Constants.ProviderFlags.MinimumVersion);

			var level = version?.Value switch
			{
				"7.1.38" => DB2iSeriesLevels.V7_1_38,
				"7.2.9" => DB2iSeriesLevels.V7_1_38,
				_ => DB2iSeriesLevels.Any
			};
			

			if (mapGuidAsString)
			{
				return level switch
				{
					DB2iSeriesLevels.V7_1_38 => new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73_GAS, level, true),
					_ => new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_GAS, level, true),
				};
			}
			else
			{
				return level switch
				{
					DB2iSeriesLevels.V7_1_38 => new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73, level, true),
					_ => new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, level, true),
				};
			}
		}
	}
}
