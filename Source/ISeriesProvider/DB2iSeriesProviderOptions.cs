namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesProviderOptions
	{
		public static class Defaults
		{
			public const DB2iSeriesVersion Version = DB2iSeriesVersion.V7_1;
			public const bool MapGuidAsString = false;
			public const DB2iSeriesProviderType ProviderType = DB2iSeriesProviderType.Odbc;

			public static DB2iSeriesProviderOptions Instance { get; set; } = new();
		}

		public DB2iSeriesProviderOptions(string providerName, DB2iSeriesProviderType providerType, DB2iSeriesVersion version, bool mapGuidAsString)
		{
			ProviderType = providerType;
			ProviderName = providerName;
			Version = version;
			MapGuidAsString = mapGuidAsString;
		}

		public DB2iSeriesProviderOptions()
			: this(DB2iSeriesProviderName.GetProviderName(
					Defaults.Version,
					Defaults.ProviderType,
					new DB2iSeriesMappingOptions(Defaults.MapGuidAsString)),
				 Defaults.ProviderType,
				 Defaults.Version,
				 Defaults.MapGuidAsString
				 )
		{
		}

		public string ProviderName { get; }
		public DB2iSeriesProviderType ProviderType { get; }
		public DB2iSeriesVersion Version { get; }
		public bool MapGuidAsString { get; }
	}
}
