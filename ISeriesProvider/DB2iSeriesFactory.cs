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
				var x when x.StartsWith("7.3.") || x == "7.3" || x == "7_3" => DB2iSeriesVersion.V7_3,
				var x when x.StartsWith("7.2.") || x == "7.2" || x == "7_2" => DB2iSeriesVersion.V7_2,
				_ => DB2iSeriesVersion.V7_1
			};
			
			var providerType = attributes.FirstOrDefault(_ => _.Name == "assemblyName")?.Value switch
			{
				DB2iSeriesAccessClientProviderAdapter.AssemblyName => DB2iSeriesAdoProviderType.AccessClient,
				DB2.DB2ProviderAdapter.AssemblyName => DB2iSeriesAdoProviderType.DB2,
				OleDbProviderAdapter.AssemblyName => DB2iSeriesAdoProviderType.OleDb,
				OdbcProviderAdapter.AssemblyName => DB2iSeriesAdoProviderType.Odbc,
				_ => DB2iSeriesAdoProviderType.Odbc
			};

			var mapGuidAsString = attributes.Any(x => x.Name == Constants.ProviderFlags.MapGuidAsString);

			return DB2iSeriesTools.GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString));
		}
	}
}
