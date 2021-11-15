using System.Collections.Generic;
using System.Linq;
using LinqToDB.Configuration;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesFactory : IDataProviderFactory
	{
		public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
		{
			var versionText = attributes.FirstOrDefault(_ => _.Name == "version");

			var version = versionText.Value switch
			{
				var x when x.StartsWith("7.4.") || x == "7.4" || x == "7_4" => DB2iSeriesVersion.V7_4,
				var x when x.StartsWith("7.3.") || x == "7.3" || x == "7_3" => DB2iSeriesVersion.V7_3,
				var x when x.StartsWith("7.2.") || x == "7.2" || x == "7_2" => DB2iSeriesVersion.V7_2,
				_ => DB2iSeriesVersion.V7_1
			};

			var providerType = attributes.FirstOrDefault(_ => _.Name == "assemblyName")?.Value switch
			{
#if NETFRAMEWORK
				DB2iSeriesAccessClientProviderAdapter.AssemblyName => DB2iSeriesProviderType.AccessClient,
#endif
				DB2.DB2ProviderAdapter.AssemblyName => DB2iSeriesProviderType.DB2,
				OleDbProviderAdapter.AssemblyName => DB2iSeriesProviderType.OleDb,
				OdbcProviderAdapter.AssemblyName => DB2iSeriesProviderType.Odbc,
				null => DB2iSeriesProviderOptions.Defaults.ProviderType,
				var x => throw ExceptionHelper.InvalidAssemblyName(x)
			};

			var mapGuidAsString = attributes.Any(x => x.Name == Constants.ProviderFlags.MapGuidAsString);

			return DB2iSeriesTools.GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString));
		}
	}
}
