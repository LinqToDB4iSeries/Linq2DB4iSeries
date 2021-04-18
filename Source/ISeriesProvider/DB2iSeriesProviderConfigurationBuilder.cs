using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesProviderConfigurationBuilder
	{
		internal DB2iSeriesProviderType ProviderType { get; set; } = DB2iSeriesProviderOptions.Defaults.ProviderType;
		internal DB2iSeriesVersion Version { get; set; } = DB2iSeriesProviderOptions.Defaults.Version;
		internal DB2iSeriesMappingOptions MappingOptions { get; set; } = DB2iSeriesMappingOptions.Default;

		public DB2iSeriesProviderConfigurationBuilder WithProviderType(DB2iSeriesProviderType providerType)
		{
			ProviderType = providerType;
			return this;
		}

		public DB2iSeriesProviderConfigurationBuilder WithVersion(DB2iSeriesVersion version)
		{
			Version = version;
			return this;
		}
		public DB2iSeriesProviderConfigurationBuilder WithMappingOptions(DB2iSeriesMappingOptions mappingOptions)
		{
			MappingOptions = mappingOptions ?? throw new ArgumentNullException(nameof(mappingOptions));
			return this;
		}
	}
}
