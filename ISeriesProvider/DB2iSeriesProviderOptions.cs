namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesProviderOptions
	{
		public static class Defaults
		{
			public const DB2iSeriesVersion Version = DB2iSeriesVersion.V7_1;
			public const bool MapGuidAsString = false;
			public const DB2iSeriesAdoProviderType ProviderType = DB2iSeriesAdoProviderType.Odbc;

			public static DB2iSeriesProviderOptions Instance = new DB2iSeriesProviderOptions();
		}

		public DB2iSeriesProviderOptions(string providerName, DB2iSeriesAdoProviderType providerType)
		{
			ProviderType = providerType;
			ProviderName = providerName;
		}

		public DB2iSeriesProviderOptions(string providerName, DB2iSeriesAdoProviderType providerType, DB2iSeriesVersion version)
			: this(providerName, providerType)
		{
			SupportsOffsetClause = version > DB2iSeriesVersion.V7_2;
			SupportsTruncateTable = version > DB2iSeriesVersion.V7_1;
		}

		public DB2iSeriesProviderOptions()
			: this(DB2iSeriesProviderName.GetProviderName(
					Defaults.Version,
					Defaults.ProviderType,
					new DB2iSeriesMappingOptions(Defaults.MapGuidAsString)),
				 Defaults.ProviderType
				 )
		{
		}

		public string ProviderName { get; set; }
		public DB2iSeriesAdoProviderType ProviderType { get; }
		public bool SupportsOffsetClause { get; set; }
		public bool SupportsTruncateTable { get; set; }
		public bool MapGuidAsString { get; set; }
	}
}
