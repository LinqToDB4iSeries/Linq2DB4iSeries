using System;

using LinqToDB.Data;
using LinqToDB.Internal.DataProvider;
using LinqToDB.Mapping;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class DB2iSeriesMultipleRowsHelper<T> : MultipleRowsHelper<T>
		where T : notnull
	{
		private readonly DB2iSeriesSqlProviderFlags db2ISeriesSqlProviderFlags;
		
		public DB2iSeriesMultipleRowsHelper(ITable<T> table, DataOptions options, DB2iSeriesSqlProviderFlags db2ISeriesSqlProviderFlags)
			: base(table, options)
		{
			this.db2ISeriesSqlProviderFlags = db2ISeriesSqlProviderFlags;
		}

		public override void BuildColumns(object item, bool castParameters = false, bool castAllRows = false, bool castFirstRowLiteralOnUnionAll = false, Func<ColumnDescriptor, bool>? castLiteral = null)
		{
			for (var i = 0; i < Columns.Length; i++)
			{
				var column = Columns[i];
				var value = column.GetProviderValue(item);
				var columnType = ColumnTypes[i];
				
				// Get the type to cast to
				var dbDataType = MappingSchema.GetDbTypeForCast(columnType, value, db2ISeriesSqlProviderFlags);
				dbDataType = MappingSchema.SanitizeDbDataType(dbDataType, db2ISeriesSqlProviderFlags);
				var casttype = dbDataType.DbType;

				if (value == null)
				{
					StringBuilder.Append(FormattableString.Invariant($"CAST(NULL AS {casttype})"));
				}
				else if (!MappingSchema.ValueToSqlConverter.TryConvert(StringBuilder, DataConnection.MappingSchema, columnType, this.Options, value))
				{
					if (value is DataParameter parameter)
						value = parameter.Value;
					
					var dataParameter = new DataParameter("p" + ParameterIndex, value, column.DataType);

					Parameters.Add(dataParameter);

					var parameterMarker = SqlBuilder.ConvertInline(dataParameter.Name!, Internal.SqlProvider.ConvertType.NameToQueryParameter);

					var nameWithCast = casttype is null ?
						parameterMarker :
						$"CAST({parameterMarker} AS {casttype})";
					
					StringBuilder.Append(nameWithCast);
				}

				if (i < Columns.Length - 1)
					StringBuilder.Append(", ");
			}
		}
	}
}

