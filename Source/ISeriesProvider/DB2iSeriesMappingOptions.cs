namespace LinqToDB.DataProvider.DB2iSeries
{
	using SqlProvider;

	public class DB2iSeriesMappingOptions
	{
		public static readonly DB2iSeriesMappingOptions Default = new DB2iSeriesMappingOptions(DB2iSeriesProviderOptions.Defaults.MapGuidAsString);

		public DB2iSeriesMappingOptions(bool mapGuidAsString)
		{
			MapGuidAsString = mapGuidAsString;
		}

		public DB2iSeriesMappingOptions(DB2iSeriesProviderOptions providerOptions)
			: this(providerOptions.MapGuidAsString)
		{

		}

		public bool MapGuidAsString { get; }

		public void SetCustomFlags(SqlProviderFlags sqlProviderFlags)
		{
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.MapGuidAsString, MapGuidAsString);
		}
	}
}
