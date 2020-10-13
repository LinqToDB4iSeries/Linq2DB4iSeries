using System;
using System.Collections.Generic;
using System.Data;
using LinqToDB.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Mapping;
	using SchemaProvider;
	using SqlProvider;
	using System.Linq.Expressions;
	using System.Threading;
	using System.Threading.Tasks;

	public class DB2iSeriesODBCDataProvider : DynamicDataProviderBase<OdbcProviderAdapter>, IDB2iSeriesDataProvider
	{
		IDynamicProviderAdapter IDB2iSeriesDataProvider.Adapter => Adapter;
		DB2iSeriesAdoProviderType IDB2iSeriesDataProvider.ProviderType => DB2iSeriesAdoProviderType.Odbc;

		readonly DB2iSeriesSqlOptimizer sqlOptimizer;
		readonly DB2iSeriesSchemaProvider schemaProvider;

		private readonly DB2iSeriesLevels minLevel;
		private readonly bool mapGuidAsString;

		public DB2iSeriesODBCDataProvider() : this(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false)
		{
		}

		public DB2iSeriesODBCDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString)
			: base(
				  name,
				  GetMappingSchema(name, mapGuidAsString),
				  OdbcProviderAdapter.GetInstance())
		{
			this.minLevel = minLevel;
			this.mapGuidAsString = mapGuidAsString;

			DB2iSeriesLoadExpressions.SetupExpressions(name, mapGuidAsString);

			SqlProviderFlags.AcceptsTakeAsParameter = false;
			SqlProviderFlags.AcceptsTakeAsParameterIfSkip = true;
			SqlProviderFlags.IsDistinctOrderBySupported = false;
			SqlProviderFlags.CanCombineParameters = false;
			SqlProviderFlags.IsParameterOrderDependent = true;
			SqlProviderFlags.IsCommonTableExpressionsSupported = true;
			SqlProviderFlags.IsUpdateFromSupported = false;

			if (mapGuidAsString)
				SqlProviderFlags.CustomFlags.Add(Constants.ProviderFlags.MapGuidAsString);

			SetCharField(Constants.DbTypes.Char, (r, i) => r.GetString(i).TrimEnd(' '));
			SetCharField(Constants.DbTypes.NChar, (r, i) => r.GetString(i).TrimEnd(' '));
			SetCharField(Constants.DbTypes.Graphic, (r, i) => r.GetString(i).TrimEnd(' '));

			sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags);
			schemaProvider = new DB2iSeriesSchemaProvider(this);
			bulkCopy = new DB2iSeriesBulkCopy(this);
		}

		protected override string NormalizeTypeName(string typeName)
		{
			if (typeName.StartsWith("GRAPHIC"))
				return "NCHAR";

			if (typeName.StartsWith("VARGRAPHIC"))
				return "NVARCHAR";

			return base.NormalizeTypeName(typeName);
		}

		public override Expression GetReaderExpression(IDataReader reader, int idx, Expression readerExpression, Type toType)
		{
			reader = reader is System.Data.Common.DbDataReader odbcDataReader 
				&& reader.GetType().Name == "OdbcDataReader" ?
				new OdbcDataReaderWrapper(odbcDataReader) : reader;

			return base.GetReaderExpression(reader, idx, readerExpression, toType);
		}

		public override bool? IsDBNullAllowed(IDataReader reader, int idx)
		{
			return true;
		}

		public override ISqlBuilder CreateSqlBuilder(MappingSchema mappingSchema)
		{
			return minLevel == DB2iSeriesLevels.V7_1_38 ?
				new DB2iSeriesSqlBuilder7_2(this, mappingSchema, GetSqlOptimizer(), SqlProviderFlags) :
				new DB2iSeriesSqlBuilder(this, mappingSchema, GetSqlOptimizer(), SqlProviderFlags);
		}

		public override ISchemaProvider GetSchemaProvider()
		{
			return schemaProvider;
		}

		public override ISqlOptimizer GetSqlOptimizer()
		{
			return sqlOptimizer;
		}

		public override void InitCommand(DataConnection dataConnection, CommandType commandType, string commandText, DataParameter[] parameters, bool withParameters)
		{
			dataConnection.DisposeCommand();
			base.InitCommand(dataConnection, commandType, commandText, parameters, withParameters);
		}

		private static MappingSchema GetMappingSchema(string configuration, bool mapGuidAsString)
		{
			return mapGuidAsString
				? (MappingSchema)new DB2iSeriesGuidAsStringMappingSchema(configuration)
				: new DB2iSeriesMappingSchema(configuration);
		}

		public override void SetParameter(DataConnection dataConnection, IDbDataParameter parameter, string name, DbDataType dataType, object value)
		{
			switch (dataType.DataType)
			{
				case DataType.Byte:
				case DataType.SByte:
				case DataType.Boolean:
				case DataType.Int16:
					dataType = dataType.WithDataType(DataType.Int16);
					value = DataTypeConverter.TryConvertOrOriginal(value, dataType.DataType);
					break;
				case DataType.Int32:
				case DataType.UInt16:
					dataType = dataType.WithDataType(DataType.Int32);
					value = DataTypeConverter.TryConvertOrOriginal(value, dataType.DataType);
					break;
				case DataType.Int64:
				case DataType.UInt32:
					dataType = dataType.WithDataType(DataType.Int64);
					value = DataTypeConverter.TryConvertOrOriginal(value, dataType.DataType);
					break;
				case DataType.VarNumeric:
				case DataType.Decimal:
				case DataType.UInt64:
					dataType = dataType.WithDataType(DataType.Decimal);
					value = DataTypeConverter.TryConvertOrOriginal(value, dataType.DataType);
					break;
				case DataType.Single:
				case DataType.Double:
					value = DataTypeConverter.TryConvertOrOriginal(value, dataType.DataType);
					break;

				case DataType.Char:
				case DataType.VarChar:
				case DataType.NChar:
				case DataType.NVarChar:
				case DataType.Text:
				case DataType.NText:
					if (value is Guid textGuid) value = textGuid.ToString();
					else if (value is bool textBool) value = ConvertTo<char>.From(textBool);
					break;

				case DataType.Guid:
					dataType = dataType.WithDataType(
						mapGuidAsString ? DataType.NVarChar : DataType.VarBinary);

					if (value is Guid guid)
						value = mapGuidAsString ?
							(object)guid.ToString() : guid.ToByteArray();

					break;
				case DataType.Binary:
				case DataType.VarBinary:
					if (value is Guid varBinaryGuid) value = varBinaryGuid.ToByteArray();
					break;

				case DataType.DateTime2:
					dataType = dataType.WithDataType(DataType.DateTime);
					break;

				case DataType.Time:
					value = value switch
					{
						DateTime dateTime => dateTime.TimeOfDay,
						DateTimeOffset dateTimeOffset => dateTimeOffset.TimeOfDay,
						_ => value
					};
					break;
			}

			base.SetParameter(dataConnection, parameter, "?", dataType, value);
		}

		protected override void SetParameterType(DataConnection dataConnection, IDbDataParameter parameter, DbDataType dataType)
		{
			OdbcProviderAdapter.OdbcType? type = null;
			switch (dataType.DataType)
			{
				case DataType.Blob: type = OdbcProviderAdapter.OdbcType.VarBinary; break;
			}

			if (type != null)
			{
				var param = TryGetProviderParameter(parameter, dataConnection.MappingSchema);
				if (param != null)
				{
					Adapter.SetDbType(param, type.Value);
					return;
				}
			}

			switch (dataType.DataType)
			{
				case DataType.Byte:
				case DataType.SByte:
				case DataType.UInt16: parameter.DbType = DbType.Int32; return;
				case DataType.UInt32: parameter.DbType = DbType.Int64; return;
				case DataType.UInt64: parameter.DbType = DbType.Decimal; return;
			}

			base.SetParameterType(dataConnection, parameter, dataType);
		}

		public bool TryGetProviderParameterName(IDbDataParameter parameter, MappingSchema mappingSchema, out string name)
		{
			var param = TryGetProviderParameter(parameter, MappingSchema);
			if (param != null)
			{
				name = Adapter.GetDbType(param).ToString();
				return true;
			}

			name = null;
			return false;
		}

		public bool TryGetProviderConnection(IDbConnection connection, MappingSchema mappingSchema, out IDbConnection providerConnection)
		{
			providerConnection = TryGetProviderConnection(connection, mappingSchema);
			return providerConnection != null;
		}

		#region BulkCopy

		DB2iSeriesBulkCopy bulkCopy;

		public override BulkCopyRowsCopied BulkCopy<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source)
		{
			return bulkCopy.BulkCopy(options.BulkCopyType.GetEffectiveType(), table, options, source);
		}

		public override Task<BulkCopyRowsCopied> BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			return bulkCopy.BulkCopyAsync(options.BulkCopyType.GetEffectiveType(), table, options, source, cancellationToken);
		}

#if !NETFRAMEWORK
		public override Task<BulkCopyRowsCopied> BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			return bulkCopy.BulkCopyAsync(options.BulkCopyType.GetEffectiveType(), table, options, source, cancellationToken);
		}
#endif
		#endregion
	}
}
