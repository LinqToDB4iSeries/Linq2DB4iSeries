namespace LinqToDB.DataProvider.DB2iSeries
{
    using SqlProvider;

    public class DB2iSeriesSqlBuilder7_2 : DB2iSeriesSqlBuilder
    {
	    public DB2iSeriesSqlBuilder7_2(ISqlOptimizer sqlOptimizer, SqlProviderFlags sqlProviderFlags, ValueToSqlConverter valueToSqlConverter) : base(sqlOptimizer, sqlProviderFlags, valueToSqlConverter)
	    {
	    }

	    protected override string OffsetFormat
        {
            get
            {
                return "OFFSET {0} ROWS";
            }
        }

        protected override bool OffsetFirst
        {
            get
            {
                return true;
            }
        }

        protected override string LimitFormat
        {
            get
            {
                return "FETCH FIRST {0} ROWS ONLY";
            }
        }

        protected override void BuildSql()
        {
			DefaultBuildSqlMethod();
        }
    }
}