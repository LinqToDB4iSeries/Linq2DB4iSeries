using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
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
		
		//TODO: v3.6.0 check if default implenetation with castParameters works?
		public override void BuildColumns(object item, Func<ColumnDescriptor, bool> skipConvert = null, bool castParameters = false, bool castAllRows = false)
		{
			skipConvert ??= (_ => false);

			for (var i = 0; i < Columns.Length; i++)
			{
				var column = Columns[i];
				var value = column.GetValue(item);
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
				else if (!skipConvert(column) && !ValueConverter.TryConvert(StringBuilder, columnType, value))
				{
					var name = ParameterName == "?" ? ParameterName : ParameterName + ++ParameterIndex;

					if (value is DataParameter parameter)
					{
						value = parameter.Value;
					}

					var dataParameter = new DataParameter(ParameterName == "?" ? ParameterName : "p" + ParameterIndex, value, column.DataType);

					Parameters.Add(dataParameter);

					var parameterMarker = dataParameter.Name == "?" || string.IsNullOrEmpty(dataParameter.Name) ?
						"?" : $"@{dataParameter.Name}";

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

