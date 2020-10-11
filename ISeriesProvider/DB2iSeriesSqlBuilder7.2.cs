namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Mapping;
	using LinqToDB.SqlQuery;
	using SqlProvider;
	
	public class DB2iSeriesSqlBuilder7_2 : DB2iSeriesSqlBuilder
	{
		public DB2iSeriesSqlBuilder7_2(
			IDB2iSeriesDataProvider provider,
			MappingSchema mappingSchema,
			ISqlOptimizer sqlOptimizer,
			SqlProviderFlags sqlProviderFlags)
			: base(provider, mappingSchema, sqlOptimizer, sqlProviderFlags)
		{
		}

		// remote context
		public DB2iSeriesSqlBuilder7_2(
			MappingSchema mappingSchema,
			ISqlOptimizer sqlOptimizer,
			SqlProviderFlags sqlProviderFlags)
			: base(mappingSchema, sqlOptimizer, sqlProviderFlags)
		{
		}

		protected override ISqlBuilder CreateSqlBuilder()
		{
			return new DB2iSeriesSqlBuilder7_2(Provider, MappingSchema, SqlOptimizer, SqlProviderFlags);
		}

		protected override string OffsetFormat(SelectQuery selectQuery) => "OFFSET {0} ROWS";

		protected override bool OffsetFirst => true;

		protected override string LimitFormat(SelectQuery selectQuery) => "FETCH FIRST {0} ROWS ONLY";

		protected override void BuildTruncateTableStatement(SqlTruncateTableStatement truncateTable)
		{
			var table = truncateTable.Table;

			AppendIndent();
			StringBuilder.Append("TRUNCATE TABLE ");
			BuildPhysicalTable(table, null);

			if (truncateTable.ResetIdentity)
				StringBuilder.Append(" RESTART IDENTITY");
		}

		protected override void BuildCommand(SqlStatement statement, int commandNumber)
		{
			if (statement is SqlTruncateTableStatement)
				return;

			base.BuildCommand(statement, commandNumber);
		}
	}
}
