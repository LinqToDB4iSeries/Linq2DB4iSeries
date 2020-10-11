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

	public enum DB2iSeriesAdoProviderType
	{
		AccessClient,
		Odbc,
		OleDb,
		DB2
	}

	public interface IDB2iSeriesDataProvider : IDataProvider
	{
		DB2iSeriesAdoProviderType ProviderType { get; }
		bool TryGetProviderParameterName(IDbDataParameter parameter, MappingSchema mappingSchema, out string name);
		bool TryGetProviderConnection(IDbConnection connection, MappingSchema mappingSchema, out IDbConnection dataConnection);
		IDbConnection TryGetProviderConnection(IDbConnection connection, MappingSchema mappingSchema);
		IDynamicProviderAdapter Adapter { get; }
	}

	public class DB2iSeriesDataProvider : DynamicDataProviderBase<DB2iSeriesProviderAdapter>, IDB2iSeriesDataProvider
	{
		IDynamicProviderAdapter IDB2iSeriesDataProvider.Adapter => Adapter;
		DB2iSeriesAdoProviderType IDB2iSeriesDataProvider.ProviderType => DB2iSeriesAdoProviderType.AccessClient;

		readonly DB2iSeriesSqlOptimizer sqlOptimizer;
		readonly DB2iSeriesSchemaProvider schemaProvider;

		private readonly DB2iSeriesLevels minLevel;
		private readonly bool mapGuidAsString;

		public DB2iSeriesDataProvider() : this(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false)
		{
		}

		public DB2iSeriesDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString)
			: base(
				  name,
				  GetMappingSchema(name, mapGuidAsString, DB2iSeriesProviderAdapter.GetInstance().MappingSchema),
				  DB2iSeriesProviderAdapter.GetInstance())
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

			sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags);
			schemaProvider = new DB2iSeriesSchemaProvider(this);
			bulkCopy = new DB2iSeriesBulkCopy(this);
			
			SetProviderField(Adapter.iDB2BigIntType, typeof(long), Adapter.GetiDB2BigIntReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2BinaryType, typeof(byte[]), Adapter.GetiDB2BinaryReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2BlobType, typeof(byte[]), Adapter.GetiDB2BlobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2CharType, typeof(string), Adapter.GetiDB2CharReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2CharBitDataType, typeof(byte[]), Adapter.GetiDB2CharBitDataReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2ClobType, typeof(string), Adapter.GetiDB2ClobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DataLinkType, typeof(string), Adapter.GetiDB2DataLinkReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DateType, typeof(DateTime), Adapter.GetiDB2DateReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DbClobType, typeof(string), Adapter.GetiDB2DbClobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DecFloat16Type, typeof(decimal), Adapter.GetiDB2DecFloat16ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DecFloat34Type, typeof(decimal), Adapter.GetiDB2DecFloat34ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DecimalType, typeof(decimal), Adapter.GetiDB2DecimalReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DoubleType, typeof(double), Adapter.GetiDB2DoubleReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2GraphicType, typeof(string), Adapter.GetiDB2GraphicReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2IntegerType, typeof(int), Adapter.GetiDB2IntegerReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2NumericType, typeof(decimal), Adapter.GetiDB2NumericReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2RealType, typeof(float), Adapter.GetiDB2RealReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2RowidType, typeof(byte[]), Adapter.GetiDB2RowidReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2SmallIntType, typeof(short), Adapter.GetiDB2SmallIntReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2TimeType, typeof(DateTime), Adapter.GetiDB2TimeReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2TimeStampType, typeof(DateTime), Adapter.GetiDB2TimeStampReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarBinaryType, typeof(byte[]), Adapter.GetiDB2VarBinaryReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarCharType, typeof(string), Adapter.GetiDB2VarCharReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarCharBitDataType, typeof(byte[]), Adapter.GetiDB2VarCharBitDataReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarGraphicType, typeof(string), Adapter.GetiDB2VarGraphicReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2XmlType, typeof(string), Adapter.GetiDB2XmlReaderMethod, dataReaderType: Adapter.DataReaderType);
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
					//Time parameters will only accept iDb2Time or string representation of time
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
			DB2iSeriesProviderAdapter.iDB2DbType? type = null;
			switch (dataType.DataType)
			{
				case DataType.Blob: type = DB2iSeriesProviderAdapter.iDB2DbType.iDB2Blob; break;
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
