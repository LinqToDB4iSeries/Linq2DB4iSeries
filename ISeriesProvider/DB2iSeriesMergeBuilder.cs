using System.Data.SqlTypes;
using LinqToDB.Data;
using LinqToDB.Mapping;
using LinqToDB.SqlProvider;
using LinqToDB.SqlQuery;

namespace LinqToDB.DataProvider.DB2iSeries
{
    class DB2iSeriesMergeBuilder<TTarget, TSource> : BasicMergeBuilder<TTarget, TSource>
        where TTarget : class
        where TSource : class
    {
        public DB2iSeriesMergeBuilder(DataConnection connection, IMergeable<TTarget, TSource> merge)
            : base(connection, merge)
        {
        }

        protected override bool ProviderUsesAlternativeUpdate => true;

        protected override void AddSourceValue(ValueToSqlConverter valueConverter, ColumnDescriptor column, SqlDataType columnType, object value, bool isFirstRow)
        {
            if (value == null || value is INullable && ((INullable)value).IsNull)
            {
                string colType = "CHAR";

                if (column.MemberType != null)
                {
                    var actualType = SqlDataType.GetDataType(column.MemberType);

                    colType = DB2iSeriesMappingSchema.GetiSeriesType(actualType);
                }

                Command.AppendFormat("CAST(NULL AS {0})", colType);
                return;
            }

            base.AddSourceValue(valueConverter, column, columnType, value, isFirstRow);
        }
    }
}
