using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using SqlProvider;
    using SqlQuery;
    using System.Data;

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

	    protected override bool OffsetFirst { get; } = true;

	    protected override string LimitFormat { get; } = "FETCH FIRST {0} ROWS ONLY"; 

        protected override void BuildSql()
        {
			DefaultBuildSqlMethod();
        }
    }
}