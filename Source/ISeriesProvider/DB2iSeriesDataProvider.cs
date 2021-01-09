using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Mapping;
	using SchemaProvider;
	using SqlProvider;
	using Common;
	using static DataProvider.OleDbProviderAdapter;
	using static DataProvider.OdbcProviderAdapter;
	using static DataProvider.DB2.DB2ProviderAdapter;
#if NETFRAMEWORK
	using static DB2iSeriesAccessClientProviderAdapter;
#endif

	public class DB2iSeriesDataProvider : DynamicDataProviderBase<DB2iSeriesProviderAdapter>
	{
		public DB2iSeriesProviderType ProviderType { get; }

		private readonly DB2iSeriesSqlOptimizer sqlOptimizer;
		private readonly DB2iSeriesSchemaProvider schemaProvider;
		private readonly DB2iSeriesBulkCopy bulkCopy;
		private readonly DB2iSeriesSqlProviderFlags db2iSeriesSqlProviderFlags;
		private readonly DB2iSeriesMappingOptions mappingOptions;

		/// <summary>
		/// Build a DB2 iSeries data provider with default options. The name will be infered from options and will be one of the constants in the DB2iSeriesProviderName class
		/// </summary>
		public DB2iSeriesDataProvider() : 
			this(DB2iSeriesProviderOptions.Defaults.Instance)
		{
		}


		/// <summary>
		/// Build a provider for a spefic configuration. Please check the names available in the DB2iSeriesProviderName class
		/// </summary>
		/// <param name="name">The name of the configuration</param>
		public DB2iSeriesDataProvider(string name)
			:this(DB2iSeriesProviderName.GetProviderOptions(name))
		{
			
		}

		/// <summary>
		/// Build a DB2 iSeries data provider. The name will be infered from options and will be one of the constants in the DB2iSeriesProviderName class
		/// </summary>
		/// <param name="providerType">Undelying Ado.Net provider type</param>
		/// <param name="version">iSeries version</param>
		/// <param name="mappingOptions">Mapping specific options</param>
		public DB2iSeriesDataProvider(
			DB2iSeriesProviderType providerType = DB2iSeriesProviderOptions.Defaults.ProviderType,
			DB2iSeriesVersion version = DB2iSeriesProviderOptions.Defaults.Version,
			DB2iSeriesMappingOptions mappingOptions = null)
			: this(DB2iSeriesProviderName.GetProviderName(version, providerType, mappingOptions ?? DB2iSeriesMappingOptions.Default))
		{

		}

		/// <summary>
		/// Build a DB2 iSeries data provider.
		/// </summary>
		/// <param name="name">Configuration name</param>
		/// <param name="providerType">Undelying Ado.Net provider type</param>
		/// <param name="version">iSeries version</param>
		/// <param name="mappingOptions">Mapping specific options</param>
		public DB2iSeriesDataProvider(
			string name,
			DB2iSeriesProviderType providerType = DB2iSeriesProviderOptions.Defaults.ProviderType,
			DB2iSeriesVersion version = DB2iSeriesProviderOptions.Defaults.Version,
			DB2iSeriesMappingOptions mappingOptions = null)
			: this(new DB2iSeriesProviderOptions(name, providerType, version)
			{
				MapGuidAsString = mappingOptions?.MapGuidAsString ?? DB2iSeriesProviderOptions.Defaults.MapGuidAsString
			})
		{

		}

		/// <summary>
		/// Build a DB2 iSeries data provider.
		/// </summary>
		/// <param name="providerOptions">The provider's construction options, see <see cref="DB2iSeriesProviderOptions"/></param>
		public DB2iSeriesDataProvider(DB2iSeriesProviderOptions providerOptions)
			: base(
				  providerOptions.ProviderName,
				  GetMappingSchema(
					  providerOptions.ProviderName,
					  providerOptions.ProviderType,
					  providerOptions.MapGuidAsString),
				  DB2iSeriesProviderAdapter.GetInstance(providerOptions.ProviderType))
		{
			this.db2iSeriesSqlProviderFlags = new DB2iSeriesSqlProviderFlags(providerOptions);
			this.mappingOptions = new DB2iSeriesMappingOptions(providerOptions);
			this.ProviderType = providerOptions.ProviderType;

			DB2iSeriesLoadExpressions.SetupExpressions(providerOptions.ProviderName, mappingOptions.MapGuidAsString);

			SqlProviderFlags.AcceptsTakeAsParameter = false;
			SqlProviderFlags.AcceptsTakeAsParameterIfSkip = true;
			SqlProviderFlags.IsDistinctOrderBySupported = false;
			SqlProviderFlags.CanCombineParameters = false;
			SqlProviderFlags.IsCommonTableExpressionsSupported = true;
			SqlProviderFlags.IsUpdateFromSupported = false;
			
			db2iSeriesSqlProviderFlags.SetCustomFlags(SqlProviderFlags);
			mappingOptions.SetCustomFlags(SqlProviderFlags);

			SetCharField(Constants.DbTypes.Char, (r, i) => r.GetString(i).TrimEnd(' '));
			SetCharField(Constants.DbTypes.NChar, (r, i) => r.GetString(i).TrimEnd(' '));
			SetCharField(Constants.DbTypes.Graphic, (r, i) => r.GetString(i).TrimEnd(' '));

			sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags, db2iSeriesSqlProviderFlags);
			schemaProvider = new DB2iSeriesSchemaProvider(this);
			bulkCopy = new DB2iSeriesBulkCopy(this);

			if (ProviderType.IsOdbc())
				SetupOdbc();
			else if (ProviderType.IsOleDb())
				SetupOleDb();
#if NETFRAMEWORK
			else if (ProviderType.IsAccessClient())
				SetupAccessClient();
#endif
			else if (ProviderType.IsDB2())
				SetupDB2Connect();
			
		}

#if NETFRAMEWORK
		private void SetupAccessClient()
		{
			SqlProviderFlags.IsParameterOrderDependent = true;

			var adapter = (DB2iSeriesAccessClientProviderAdapter)Adapter.GetInstance();

			SetProviderField(adapter.iDB2BigIntType, typeof(long), adapter.GetiDB2BigIntReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2BinaryType, typeof(byte[]), adapter.GetiDB2BinaryReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2BlobType, typeof(byte[]), adapter.GetiDB2BlobReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2CharType, typeof(string), adapter.GetiDB2CharReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2CharBitDataType, typeof(byte[]), adapter.GetiDB2CharBitDataReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2ClobType, typeof(string), adapter.GetiDB2ClobReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2DataLinkType, typeof(string), adapter.GetiDB2DataLinkReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2DateType, typeof(DateTime), adapter.GetiDB2DateReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2DbClobType, typeof(string), adapter.GetiDB2DbClobReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2DecFloat16Type, typeof(decimal), adapter.GetiDB2DecFloat16ReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2DecFloat34Type, typeof(decimal), adapter.GetiDB2DecFloat34ReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2DecimalType, typeof(decimal), adapter.GetiDB2DecimalReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2DoubleType, typeof(double), adapter.GetiDB2DoubleReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2GraphicType, typeof(string), adapter.GetiDB2GraphicReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2IntegerType, typeof(int), adapter.GetiDB2IntegerReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2NumericType, typeof(decimal), adapter.GetiDB2NumericReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2RealType, typeof(float), adapter.GetiDB2RealReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2RowidType, typeof(byte[]), adapter.GetiDB2RowidReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2SmallIntType, typeof(short), adapter.GetiDB2SmallIntReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2TimeType, typeof(DateTime), adapter.GetiDB2TimeReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2TimeStampType, typeof(DateTime), adapter.GetiDB2TimeStampReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2VarBinaryType, typeof(byte[]), adapter.GetiDB2VarBinaryReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2VarCharType, typeof(string), adapter.GetiDB2VarCharReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2VarCharBitDataType, typeof(byte[]), adapter.GetiDB2VarCharBitDataReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2VarGraphicType, typeof(string), adapter.GetiDB2VarGraphicReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.iDB2XmlType, typeof(string), adapter.GetiDB2XmlReaderMethod, dataReaderType: adapter.DataReaderType);
		}
#endif

		private void SetupDB2Connect()
		{
			SqlProviderFlags.IsParameterOrderDependent = false;

			SetCharFieldToType<char>(Constants.DbTypes.Char, (r, i) => DataTools.GetChar(r, i));

			var adapter = (DB2.DB2ProviderAdapter)Adapter.GetInstance();

			SetProviderField(adapter.DB2Int64Type, typeof(long), adapter.GetDB2Int64ReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2Int32Type, typeof(int), adapter.GetDB2Int32ReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2Int16Type, typeof(short), adapter.GetDB2Int16ReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2DecimalType, typeof(decimal), adapter.GetDB2DecimalReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2DecimalFloatType, typeof(decimal), adapter.GetDB2DecimalFloatReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2RealType, typeof(float), adapter.GetDB2RealReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2Real370Type, typeof(float), adapter.GetDB2Real370ReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2DoubleType, typeof(double), adapter.GetDB2DoubleReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2StringType, typeof(string), adapter.GetDB2StringReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2ClobType, typeof(string), adapter.GetDB2ClobReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2BinaryType, typeof(byte[]), adapter.GetDB2BinaryReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2BlobType, typeof(byte[]), adapter.GetDB2BlobReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2DateType, typeof(DateTime), adapter.GetDB2DateReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2TimeType, typeof(TimeSpan), adapter.GetDB2TimeReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2TimeStampType, typeof(DateTime), adapter.GetDB2TimeStampReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2XmlType, typeof(string), adapter.GetDB2XmlReaderMethod, dataReaderType: adapter.DataReaderType);
			SetProviderField(adapter.DB2RowIdType, typeof(byte[]), adapter.GetDB2RowIdReaderMethod, dataReaderType: adapter.DataReaderType);
		}

		private void SetupOdbc()
		{
			SqlProviderFlags.IsParameterOrderDependent = true;
		}

		private void SetupOleDb()
		{
			SqlProviderFlags.IsParameterOrderDependent = true;
		}

		public override ISqlBuilder CreateSqlBuilder(MappingSchema mappingSchema)
		{
			return new DB2iSeriesSqlBuilder(this, mappingSchema, GetSqlOptimizer(), SqlProviderFlags)
			{
				DB2iSeriesSqlProviderFlags = db2iSeriesSqlProviderFlags
			};
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

		private static MappingSchema GetMappingSchema(string configuration, DB2iSeriesProviderType providerType, bool mapGuidAsString)
		{
			var providerSchema = providerType switch
			{
#if NETFRAMEWORK
				DB2iSeriesProviderType.AccessClient => DB2iSeriesAccessClientProviderAdapter.GetInstance().MappingSchema,
#endif
				DB2iSeriesProviderType.DB2 => DB2.DB2ProviderAdapter.GetInstance().MappingSchema,
				_ => new MappingSchema()
			};

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
						mappingOptions.MapGuidAsString ? DataType.NVarChar : DataType.VarBinary);

					if (value is Guid guid)
						value = mappingOptions.MapGuidAsString ?
							(object)guid.ToString() : guid.ToByteArray();

					break;
				case DataType.Binary:
				case DataType.VarBinary:
					if (value is Guid varBinaryGuid) value = varBinaryGuid.ToByteArray();
					break;

				case DataType.DateTime2:
					dataType = dataType.WithDataType(DataType.DateTime);
					break;
#if NETFRAMEWORK
				case DataType.Date:

					if (ProviderType.IsAccessClient())
					{
						//Date parameters will only accept iDb2Date or string representation of time
						value = value switch
						{
							DateTime dateTime => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Date, dateTime, false),
							DateTimeOffset dateTimeOffset => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Date, dateTimeOffset.DateTime, false),
							_ => value
						};
					}
					break;
#endif
				case DataType.Time:
					if (ProviderType.IsIBM())
					{
						//Time parameters will only accept iDb2Time/DB2Time or string representation of time
						value = value switch
						{
							TimeSpan timeSpan => DB2iSeriesSqlBuilder.ConvertTimeToSql(timeSpan, false),
							DateTime dateTime => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Time, dateTime, false),
							DateTimeOffset dateTimeOffset => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Time, dateTimeOffset.DateTime, false),
							_ => value
						};
					}
					else
					{
						value = value switch
						{
							DateTime dateTime => dateTime.TimeOfDay,
							DateTimeOffset dateTimeOffset => dateTimeOffset.TimeOfDay,
							_ => value
						};
					}
					break;
			}

			var parameterMarker = db2iSeriesSqlProviderFlags.SupportsNamedParameters ? "@" + name : "?";

			base.SetParameter(dataConnection, parameter, parameterMarker, dataType, value);
		}

		private object GetParameterProviderType(DataType dataType)
		{
			return ProviderType switch
			{
#if NETFRAMEWORK
				DB2iSeriesProviderType.AccessClient => dataType switch
				{
					DataType.Blob => iDB2DbType.iDB2Blob,
					_ => null
				},
#endif
				DB2iSeriesProviderType.DB2 => dataType switch
				{
					DataType.Blob => DB2Type.Blob,
					_ => null
				},
				DB2iSeriesProviderType.Odbc => dataType switch
				{
					DataType.Blob => OdbcType.VarBinary,
					_ => null
				},
				DB2iSeriesProviderType.OleDb => dataType switch
				{
					DataType.Blob => OleDbType.LongVarBinary,
					DataType.Time => OleDbType.DBTime,
					DataType.Date => OleDbType.DBDate,
					DataType.Text => OleDbType.LongVarChar,
					DataType.NText => OleDbType.LongVarWChar,
					DataType.Guid => OleDbType.VarBinary,
					DataType.UInt16 => OleDbType.Integer,
					DataType.UInt32 => OleDbType.BigInt,
					var x when
						x == DataType.Byte
					||	x == DataType.SByte
					||	x == DataType.Boolean => OleDbType.SmallInt,
					var x when
						x == DataType.UInt64
					||	x == DataType.Decimal => OleDbType.Decimal,
					var x when
						x == DataType.DateTime
					||	x == DataType.DateTime2 => OleDbType.DBTimeStamp,
					_ => null
				},
				_ => throw ExceptionHelper.InvalidAdoProvider(ProviderType)
			};
		}

		private void SetParameterDbType(DataType dataType, IDbDataParameter parameter)
		{
			if (ProviderType.IsOdbc())
			{
				switch (dataType)
				{
					case DataType.Byte:
					case DataType.SByte:
					case DataType.UInt16: parameter.DbType = DbType.Int32; return;
					case DataType.UInt32: parameter.DbType = DbType.Int64; return;
					case DataType.UInt64: parameter.DbType = DbType.Decimal; return;
				}
			}
			else if (ProviderType.IsOleDb())
			{
				switch (dataType)
				{
					case DataType.Byte:
					case DataType.SByte:
					case DataType.UInt16: parameter.DbType = DbType.Int32; return;
					case DataType.UInt32: parameter.DbType = DbType.Int64; return;
					case DataType.UInt64: parameter.DbType = DbType.Decimal; return;
					case DataType.DateTime:
					case DataType.DateTime2: parameter.DbType = DbType.DateTime; return;
					case DataType.Boolean: parameter.DbType = DbType.Int16; return;
				}
			}
		}

		protected override void SetParameterType(DataConnection dataConnection, IDbDataParameter parameter, DbDataType dataType)
		{
			var type = GetParameterProviderType(dataType.DataType);

			if (type != null)
			{
				var param = TryGetProviderParameter(parameter, dataConnection.MappingSchema);
				if (param != null)
				{
					Adapter.SetDbType(param, type);
					return;
				}
			}

			SetParameterDbType(dataType.DataType, parameter);

			base.SetParameterType(dataConnection, parameter, dataType);
		}

		public bool TryGetProviderParameterName(IDbDataParameter parameter, MappingSchema mappingSchema, out string name)
		{
			var param = TryGetProviderParameter(parameter, MappingSchema);
			if (param != null)
			{
				name = Adapter.GetDbTypeName(param);
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

		protected override string NormalizeTypeName(string typeName)
		{
			//Graphic types not supported in ODBC
			if (ProviderType.IsOdbc())
			{
				if (typeName.StartsWith(Constants.DbTypes.Graphic))
					return Constants.DbTypes.NChar;

				if (typeName.StartsWith(Constants.DbTypes.VarGraphic))
					return Constants.DbTypes.NVarChar;
			}

			return base.NormalizeTypeName(typeName);
		}

		public override Expression GetReaderExpression(IDataReader reader, int idx, Expression readerExpression, Type toType)
		{
			//Wrap OdbcDataReader to avoid exceptions on XML columns
			if (ProviderType.IsOdbc())
			{
				reader = reader is System.Data.Common.DbDataReader odbcDataReader
					&& reader.GetType().Name == "OdbcDataReader" ?
				new OdbcDataReaderWrapper(odbcDataReader) : reader;
			}

			return base.GetReaderExpression(reader, idx, readerExpression, toType);
		}

		public override bool? IsDBNullAllowed(IDataReader reader, int idx)
		{
			//Always return true on ODBC to avoid exceptions on XML columns
			if (ProviderType.IsOdbc())
				return true;

			return base.IsDBNullAllowed(reader, idx);
		}

		#region BulkCopy

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
