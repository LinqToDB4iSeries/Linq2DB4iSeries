﻿namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesProviderOptions
	{
		public static class Defaults
		{
			public const DB2iSeriesVersion Version = DB2iSeriesVersion.V7_1;
			public const bool MapGuidAsString = false;
			public const DB2iSeriesProviderType ProviderType = DB2iSeriesProviderType.Odbc;

			public static DB2iSeriesProviderOptions Instance = new DB2iSeriesProviderOptions();
		}

		public DB2iSeriesProviderOptions(string providerName, DB2iSeriesProviderType providerType)
		{
			ProviderType = providerType;
			ProviderName = providerName;
		}

		public DB2iSeriesProviderOptions(string providerName, DB2iSeriesProviderType providerType, DB2iSeriesVersion version)
			: this(providerName, providerType)
		{
			SupportsOffsetClause = version > DB2iSeriesVersion.V7_2;
			SupportsTruncateTable = version > DB2iSeriesVersion.V7_1 && !providerType.IsOdbc();
			SupportsMergeStatement = version >= DB2iSeriesVersion.V7_1;
			SupportsNCharTypes = version >= DB2iSeriesVersion.V7_1;
			SupportsDropIfExists = version >= DB2iSeriesVersion.V7_4;
			SupportsArbitraryTimeStampPrecision = version >= DB2iSeriesVersion.V7_2;
			SupportsTrimCharacters = version >= DB2iSeriesVersion.V7_2;
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
		public DB2iSeriesProviderType ProviderType { get; }
		public bool SupportsOffsetClause { get; set; }
		public bool SupportsTruncateTable { get; set; }
		public bool SupportsMergeStatement { get; set; }
		public bool SupportsNCharTypes { get; set; }
		public bool SupportsDropIfExists { get; set; }
		public bool SupportsArbitraryTimeStampPrecision { get; set; }
		public bool SupportsTrimCharacters { get; set; }
		public bool MapGuidAsString { get; set; }
	}
}
