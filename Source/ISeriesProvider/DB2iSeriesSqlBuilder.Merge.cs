using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using LinqToDB.SqlQuery;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal partial class DB2iSeriesSqlBuilder
	{
		protected override void BuildMergeStatement(SqlMergeStatement merge)
		{
			if (!DB2iSeriesSqlProviderFlags.SupportsMergeStatement)
				throw new LinqToDBException($"{Provider.Name} provider doesn't support SQL MERGE statement");

			base.BuildMergeStatement(merge);
		}

		//This is redundand as type hints/casting is always handled by the provider because iDB2 requires them everywhere
		//All tests pass if this is commented out. Consider removing in a later version.
		protected override bool IsSqlValuesTableValueTypeRequired(SqlValuesTable source, IReadOnlyList<ISqlExpression[]> rows, int row, int column)
		{
			if (row == -1)
				return true;

			var expr = rows[row][column];

			static bool mergeSourceValueTypeRequired(ISqlExpression expression)
			=> expression is SqlParameter
				|| (expression is SqlValue value && value.Value is null);
			
			if (!mergeSourceValueTypeRequired(expr))
				return false;

			//Base DB2 impl follows

			// empty source (single row with all values == NULL)
			if (row == -1)
				return true;

			// we add type hints to first row only
			if (row != 0)
				return false;

			// add type hint only if column contains NULL in all rows
			return rows.All(r => r[column] is SqlValue value && (value.Value == null
				|| (value is INullable nullable && nullable.IsNull)));
		}
	}
}
