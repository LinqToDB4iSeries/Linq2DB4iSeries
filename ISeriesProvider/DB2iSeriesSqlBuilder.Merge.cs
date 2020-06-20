using System.Data.SqlTypes;
using System.Linq;
using LinqToDB.SqlQuery;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public partial class DB2iSeriesSqlBuilder
	{
		protected override bool MergeSourceValueTypeRequired(SqlValuesTable sourceEnumerable, int row, int column)
		{
			// empty source (single row with all values == NULL)
			if (row == -1)
				return true;

			// we add type hints to first row only
			if (row != 0)
				return false;

			// add type hint only if column contains NULL in all rows
			return sourceEnumerable.Rows.All(r => r[column] is SqlValue value && (value.Value == null
				|| (value is INullable && ((INullable)value).IsNull)));
		}

		//protected override void AddSourceValue(ValueToSqlConverter valueConverter, ColumnDescriptor column, SqlDataType columnType, object value, bool isFirstRow, bool isLastRow)
		//{ 
		//	if (value == null || value is INullable && ((INullable)value).IsNull)
		//	{

		//		var casttype = (columnType.DataType == DataType.Undefined) ?
		//			((DB2iSeriesSqlBuilder)SqlBuilder).GetTypeForCast(columnType.Type):
		//			((DB2iSeriesSqlBuilder)SqlBuilder).GetiSeriesType(SqlDataType.GetDataType(columnType.DataType)); 

		//		Command.Append($"CAST(NULL AS {casttype})");
  //              return;
  //          }

	 //       // avoid parameters in source due to low limits for parameters number in providers
	 //       if (!valueConverter.TryConvert(Command, columnType, value))
	 //       {
		//        var colType = ((DB2iSeriesSqlBuilder) SqlBuilder).GetTypeForCast(value.GetType());

		//		// we have to use parameter wrapped in a cast
		//		var name = GetNextParameterName();
		//        var fullName = SqlBuilder.Convert(name, ConvertType.NameToQueryParameter).ToString();
				
		//		Command.Append($"CAST({fullName} as {colType})");

		//        AddParameter(new DataParameter(name, value, column.DataType));
		//	}
  //      }
	}
}
