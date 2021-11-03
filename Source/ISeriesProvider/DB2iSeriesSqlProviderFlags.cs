namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Tools;
	using SqlProvider;

	public class DB2iSeriesSqlProviderFlags
	{
		public DB2iSeriesSqlProviderFlags(
			bool supportsOffsetClause,
			bool supportsTruncateTable,
			bool supportsNamedParameters,
			bool supportsMergeStatement,
			bool supportsNCharTypes,
			bool supportsDropTableIfExists,
			bool supportsArbitraryTimeStampPercision)
		{
			SupportsOffsetClause = supportsOffsetClause;
			SupportsTruncateTable = supportsTruncateTable;
			SupportsNamedParameters = supportsNamedParameters;
			SupportsMergeStatement = supportsMergeStatement;
			SupportsNCharTypes = supportsNCharTypes;
			SupportsDropTableIfExists = supportsDropTableIfExists;
			SupportsArbitraryTimeStampPercision = supportsArbitraryTimeStampPercision;
		}

		public DB2iSeriesSqlProviderFlags(SqlProviderFlags sqlProviderFlags)
			:this(
				 supportsOffsetClause: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsOffsetClause),
				 supportsTruncateTable: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsTruncateTable),
				 supportsNamedParameters: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsNamedParameters),
				 supportsMergeStatement: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsMergeStatement),
				 supportsNCharTypes: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsNCharTypes),
				 supportsDropTableIfExists: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsDropTableIfExists),
				 supportsArbitraryTimeStampPercision: sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.SupportsArbitraryTimeStampPercision))
		{

		}

		public DB2iSeriesSqlProviderFlags(DB2iSeriesProviderOptions options)
			: this(
				 options.SupportsOffsetClause,
				 options.SupportsTruncateTable,
				 supportsNamedParameters: options.ProviderType.IsIBM(),
				 supportsMergeStatement: options.SupportsMergeStatement,
				 supportsNCharTypes: options.SupportsNCharTypes,
				 supportsDropTableIfExists: options.SupportsDropIfExists,
				 supportsArbitraryTimeStampPercision: options.SupportsArbitraryTimeStampPercision)
		{

		}

		public DB2iSeriesSqlProviderFlags(
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType)
			: this(
				 supportsOffsetClause: version >= DB2iSeriesVersion.V7_3,
				 supportsTruncateTable: version >= DB2iSeriesVersion.V7_2 && !providerType.IsOdbc(),
				 supportsNamedParameters: providerType.IsIBM(),
				 supportsMergeStatement: version >= DB2iSeriesVersion.V7_1,
				 supportsNCharTypes: version >= DB2iSeriesVersion.V7_1,
				 supportsDropTableIfExists: version >= DB2iSeriesVersion.V7_4,
				 supportsArbitraryTimeStampPercision: version >= DB2iSeriesVersion.V7_2
				 )
		{

		}

		public bool SupportsOffsetClause { get; }
		public bool SupportsTruncateTable { get; }
		public bool SupportsNamedParameters { get; }
		public bool SupportsMergeStatement { get; }
		public bool SupportsNCharTypes { get; }
		public bool SupportsDropTableIfExists { get; }
		public bool SupportsArbitraryTimeStampPercision { get; }

		public void SetCustomFlags(SqlProviderFlags sqlProviderFlags)
		{
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsOffsetClause, SupportsOffsetClause);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsTruncateTable, SupportsTruncateTable);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsNamedParameters, SupportsNamedParameters);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsMergeStatement, SupportsMergeStatement);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsNCharTypes, SupportsNCharTypes);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsDropTableIfExists, SupportsDropTableIfExists);
			sqlProviderFlags.SetFlag(Constants.ProviderFlags.SupportsArbitraryTimeStampPercision, SupportsArbitraryTimeStampPercision);
		}
	}
}
