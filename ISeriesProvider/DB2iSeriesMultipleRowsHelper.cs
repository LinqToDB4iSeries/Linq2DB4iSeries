using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Mapping;
	using SqlProvider;
	using SqlQuery;

	class DB2iSeriesMultipleRowsHelper<T> : MultipleRowsHelper<T>
	{
		public DB2iSeriesMultipleRowsHelper(DataConnection dataConnection, BulkCopyOptions options, bool enforceKeepIdentity) : base(dataConnection, options, enforceKeepIdentity)
		{
			
		}

		public override void BuildColumns(object item, Func<ColumnDescriptor, bool> skipConvert = null)
		{
			skipConvert = skipConvert ?? (sc => false);

			for (var i = 0; i < Columns.Length; i++)
			{
				var column = Columns[i];
				var value = column.GetValue(item);

				if (skipConvert(column) || !ValueConverter.TryConvert(StringBuilder, ColumnTypes[i], value))
				{
					var name = ParameterName == "?" ? ParameterName : ParameterName + ++ParameterIndex;

					if (value is DataParameter)
					{
						value = ((DataParameter)value).Value;
					}
					
					var dataParameter = new DataParameter(ParameterName == "?" ? ParameterName : "p" + ParameterIndex, value, column.DataType);

					Parameters.Add(dataParameter);

					// wrap the parameter with a cast
					var dbType = DataConnection.MappingSchema.GetDataType(value.GetType());
					var nameWithCast = DataConnection.MappingSchema.ValueToSqlConverter.ParameterValueExpression(dbType, "@" + dataParameter.Name);

					StringBuilder.Append(nameWithCast);
				}

				StringBuilder.Append(",");
			}

			StringBuilder.Length--;
		}
	}
}
