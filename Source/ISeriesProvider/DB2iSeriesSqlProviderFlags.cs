namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Tools;
	using SqlProvider;

	public class DB2iSeriesSqlProviderFlags
	{
		public DB2iSeriesSqlProviderFlags(
			bool supportsOffsetClause,
			bool supportsTruncateTable,
			bool supportsNamedParameters)
		{
			SupportsOffsetClause = supportsOffsetClause;
			SupportsTruncateTable = supportsTruncateTable;
			SupportsNamedParameters = supportsNamedParameters;
		}

		public DB2iSeriesSqlProviderFlags(SqlProviderFlags sqlProviderFlags)
			:this(
				 supportsOffsetClause: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsOffsetClause),
				 supportsTruncateTable: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsTruncateTable),
				 supportsNamedParameters: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsNamedParameters))
		{

		}

		public DB2iSeriesSqlProviderFlags(DB2iSeriesProviderOptions options)
			: this(
				 options.SupportsOffsetClause,
				 options.SupportsTruncateTable,
				 supportsNamedParameters: options.ProviderType.IsIBM())
		{

		}

		public DB2iSeriesSqlProviderFlags(
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType)
			: this(
				 supportsOffsetClause: version >= DB2iSeriesVersion.V7_3,
				 supportsTruncateTable: version >= DB2iSeriesVersion.V7_2,
				 supportsNamedParameters: providerType.IsIBM()
				 )
		{

		}

		public bool SupportsOffsetClause { get; }
		public bool SupportsTruncateTable { get; }
		public bool SupportsNamedParameters { get; }

		public void SetCustomFlags(SqlProviderFlags sqlProviderFlags)
		{
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsOffsetClause, SupportsOffsetClause);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsTruncateTable, SupportsTruncateTable);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsNamedParameters, SupportsNamedParameters);
		}
	}
}
