using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using LinqToDB.Common;
	using Mapping;
	using SqlQuery;

	class DB2iSeriesMultipleRowsHelper<T> : MultipleRowsHelper<T>
	{
		private readonly DB2iSeriesSqlProviderFlags db2ISeriesSqlProviderFlags;

		public DB2iSeriesMultipleRowsHelper(ITable<T> table, BulkCopyOptions options, DB2iSeriesSqlProviderFlags db2ISeriesSqlProviderFlags)
			: base(table, options)
		{
			this.db2ISeriesSqlProviderFlags = db2ISeriesSqlProviderFlags;
		}

		private static Func<ColumnDescriptor, bool> defaultSkipConvert = (_ => false);

		public override void BuildColumns(object item, Func<ColumnDescriptor, bool> skipConvert = null, bool castParameters = false, bool castAllRows = false, bool castFirstRowLiteralOnUnionAll = false, Func<ColumnDescriptor, bool> castLiteral = null)
		{
			skipConvert ??= defaultSkipConvert;

			for (var i = 0; i < Columns.Length; i++)
			{
				var column = Columns[i];
				var value = column.GetProviderValue(item);
				var columnType = ColumnTypes[i];

				if (column.DbType != null)
				{
					if (column.DbType.Equals("time", StringComparison.CurrentCultureIgnoreCase))
						columnType = new SqlDataType(DataType.Time);
					else if (column.DbType.Equals("date", StringComparison.CurrentCultureIgnoreCase))
						columnType = new SqlDataType(DataType.Date);
				}

				// wrap the parameter with a cast
				var dbType = value == null ? columnType : DataConnection.MappingSchema.GetDataType(value.GetType());
				var casttype = DataConnection.MappingSchema.GetDbTypeForCast(this.db2ISeriesSqlProviderFlags, dbType).ToSqlString();

				if (value == null)
				{
					StringBuilder.Append($"CAST(NULL AS {casttype})");
				}
				else if (!skipConvert(column) && !MappingSchema.ValueToSqlConverter.TryConvert(StringBuilder, columnType, value))
				{
					if (value is DataParameter parameter)
						value = parameter.Value;
					
					var dataParameter = new DataParameter("p" + ParameterIndex, value, column.DataType);

					Parameters.Add(dataParameter);

					var parameterMarker = SqlBuilder.ConvertInline(dataParameter.Name, SqlProvider.ConvertType.NameToQueryParameter);

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

