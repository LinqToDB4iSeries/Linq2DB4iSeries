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

        protected override void AddSourceValue(ValueToSqlConverter valueConverter, ColumnDescriptor column, SqlDataType columnType, object value, bool isFirstRow, bool isLastRow)
		{ 
			if (value == null || value is INullable && ((INullable)value).IsNull)
			{

				var casttype = (columnType.DataType == DataType.Undefined) ?
					((DB2iSeriesSqlBuilder)SqlBuilder).GetTypeForCast(columnType.Type):
					((DB2iSeriesSqlBuilder)SqlBuilder).GetiSeriesType(SqlDataType.GetDataType(columnType.DataType)); 

				Command.Append($"CAST(NULL AS {casttype})");
                return;
            }

	        // avoid parameters in source due to low limits for parameters number in providers
	        if (!valueConverter.TryConvert(Command, columnType, value))
	        {
		        var colType = ((DB2iSeriesSqlBuilder) SqlBuilder).GetTypeForCast(value.GetType());

				// we have to use parameter wrapped in a cast
				var name = GetNextParameterName();
		        var fullName = SqlBuilder.Convert(name, ConvertType.NameToQueryParameter).ToString();
		        
				Command.Append($"CAST({fullName} as {colType})");

		        AddParameter(new DataParameter(name, value, column.DataType));
			}
        }
    }
}
