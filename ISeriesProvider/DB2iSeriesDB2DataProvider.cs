using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using LinqToDB.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using LinqToDB.Mapping;
	using SchemaProvider;
	using SqlProvider;
	using System.Threading;
	using System.Threading.Tasks;

	public class DB2iSeriesDB2DataProvider : DynamicDataProviderBase<DB2.DB2ProviderAdapter>, IDB2iSeriesDataProvider
	{
		IDynamicProviderAdapter IDB2iSeriesDataProvider.Adapter => Adapter;
		DB2iSeriesAdoProviderType IDB2iSeriesDataProvider.ProviderType => DB2iSeriesAdoProviderType.AccessClient;

		readonly DB2iSeriesSqlOptimizer sqlOptimizer;
		readonly DB2iSeriesSchemaProvider schemaProvider;

		private readonly DB2iSeriesLevels minLevel;
		private readonly bool mapGuidAsString;

		public DB2iSeriesDB2DataProvider() : this(DB2iSeriesProviderName.DB2_DB2Connect, DB2iSeriesLevels.Any, false)
		{
		}

		public DB2iSeriesDB2DataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString)
			: base(
				  name,
				  GetMappingSchema(name, mapGuidAsString, DB2.DB2ProviderAdapter.GetInstance().MappingSchema),
				  DB2.DB2ProviderAdapter.GetInstance())
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

			if (mapGuidAsString)
				SqlProviderFlags.CustomFlags.Add(Constants.ProviderFlags.MapGuidAsString);

			SetCharField(Constants.DbTypes.Char, (r, i) => r.GetString(i).TrimEnd(' '));
			SetCharField(Constants.DbTypes.NChar, (r, i) => r.GetString(i).TrimEnd(' '));
			SetCharField(Constants.DbTypes.Graphic, (r, i) => r.GetString(i).TrimEnd(' '));

			SetCharFieldToType<char>(Constants.DbTypes.Char, (r, i) => DataTools.GetChar(r, i));
			
			sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags);
			schemaProvider = new DB2iSeriesSchemaProvider(this);
			bulkCopy = new DB2iSeriesBulkCopy(this);
			
			SetProviderField(Adapter.DB2Int64Type, typeof(long), Adapter.GetDB2Int64ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2Int32Type, typeof(int), Adapter.GetDB2Int32ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2Int16Type, typeof(short), Adapter.GetDB2Int16ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2DecimalType, typeof(decimal), Adapter.GetDB2DecimalReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2DecimalFloatType, typeof(decimal), Adapter.GetDB2DecimalFloatReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2RealType, typeof(float), Adapter.GetDB2RealReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2Real370Type, typeof(float), Adapter.GetDB2Real370ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2DoubleType, typeof(double), Adapter.GetDB2DoubleReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2StringType, typeof(string), Adapter.GetDB2StringReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2ClobType, typeof(string), Adapter.GetDB2ClobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2BinaryType, typeof(byte[]), Adapter.GetDB2BinaryReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2BlobType, typeof(byte[]), Adapter.GetDB2BlobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2DateType, typeof(DateTime), Adapter.GetDB2DateReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2TimeType, typeof(TimeSpan), Adapter.GetDB2TimeReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2TimeStampType, typeof(DateTime), Adapter.GetDB2TimeStampReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2XmlType, typeof(string), Adapter.GetDB2XmlReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.DB2RowIdType, typeof(byte[]), Adapter.GetDB2RowIdReaderMethod, dataReaderType: Adapter.DataReaderType);
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

		private static MappingSchema GetMappingSchema(string configuration, bool mapGuidAsString, MappingSchema providerSchema)
		{
			return mapGuidAsString
				? (MappingSchema)new DB2iSeriesGuidAsStringMappingSchema(configuration, providerSchema)
				: new DB2iSeriesMappingSchema(configuration, providerSchema);
		}

		public override void SetParameter(DataConnection dataConnection, IDbDataParameter parameter, string name, DbDataType dataType, object value)
		{
			var isDB2Type = value?.GetType().Assembly.FullName == DB2.DB2ProviderAdapter.AssemblyName;

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
					//Time parameters will only accept Db2Time or string representation of time
					value = value switch
					{
						TimeSpan timeSpan => DB2iSeriesSqlBuilder.ConvertTimeToSql(timeSpan, false),
						DateTime dateTime => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Time, dateTime, false),
						DateTimeOffset dateTimeOffset => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Time, dateTimeOffset.DateTime, false),
						_ => value
					};
					break;
			}

			base.SetParameter(dataConnection, parameter, "@" + name, dataType, value);
		}

		protected override void SetParameterType(DataConnection dataConnection, IDbDataParameter parameter, DbDataType dataType)
		{
			DB2.DB2ProviderAdapter.DB2Type? type = null;
			switch (dataType.DataType)
			{
				case DataType.Blob: type = DB2.DB2ProviderAdapter.DB2Type.Blob; break;
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
		public override BulkCopyRowsCopied BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			return _bulkCopy.BulkCopyAsync(options.BulkCopyType.GetEffectiveType(), table, options, source, cancellationToken);
		}
#endif
		#endregion
	}
}
