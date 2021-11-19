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

			SetCharFieldToType<char>(Constants.DbTypes.Char, DataTools.GetCharExpression);
			SetCharField(Constants.DbTypes.Char, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.NChar, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.Graphic, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.VarChar, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.NVarChar, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.VarGraphic, ReaderExpressionTools.GetTrimmedStringExpression);

			sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags, db2iSeriesSqlProviderFlags);
			schemaProvider = new DB2iSeriesSchemaProvider(this);
			bulkCopy = new DB2iSeriesBulkCopy(this, db2iSeriesSqlProviderFlags);

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

			//The original implementation had erronously toType and providerType reversed so the following mapping effectivily did nothing
			//Leaving commented for reference in case something needs to be readded
			//In case these are readded they override the generic char trimmings setup with SetCharField

			//SetProviderField(typeof(long), adapter.iDB2BigIntType, adapter.GetiDB2BigIntReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.iDB2BinaryType, adapter.GetiDB2BinaryReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.iDB2BlobType, adapter.GetiDB2BlobReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2CharType, adapter.GetiDB2CharReaderMethod, getTrimmedStringWrapper, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.iDB2CharBitDataType, adapter.GetiDB2CharBitDataReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2ClobType, adapter.GetiDB2ClobReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2DataLinkType, adapter.GetiDB2DataLinkReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2DbClobType, adapter.GetiDB2DbClobReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(decimal), adapter.iDB2DecFloat16Type, adapter.GetiDB2DecFloat16ReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(decimal), adapter.iDB2DecFloat34Type, adapter.GetiDB2DecFloat34ReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(decimal), adapter.iDB2DecimalType, adapter.GetiDB2DecimalReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(double), adapter.iDB2DoubleType, adapter.GetiDB2DoubleReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2GraphicType, adapter.GetiDB2GraphicReaderMethod, getTrimmedStringWrapper, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(int), adapter.iDB2IntegerType, adapter.GetiDB2IntegerReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(decimal), adapter.iDB2NumericType, adapter.GetiDB2NumericReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(float), adapter.iDB2RealType, adapter.GetiDB2RealReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.iDB2RowidType, adapter.GetiDB2RowidReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(short), adapter.iDB2SmallIntType, adapter.GetiDB2SmallIntReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(DateTime), adapter.iDB2DateType, adapter.GetiDB2DateReaderMethod, getValue<DateTime>, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(DateTime), adapter.iDB2TimeType, adapter.GetiDB2TimeReaderMethod, getValue<DateTime>, dataReaderType: adapter.DataReaderType);
			SetProviderField(typeof(DateTime), adapter.iDB2TimeStampType, adapter.GetiDB2TimeStampReaderMethod, getTimeStampWrapper(adapter.iDB2TimeStampType), dataReaderType: adapter.DataReaderType);
			SetProviderField(typeof(DateTimeOffset), adapter.iDB2TimeStampType, adapter.GetiDB2TimeStampReaderMethod, getTimeStampWrapper(adapter.iDB2TimeStampType), dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.iDB2VarBinaryType, adapter.GetiDB2VarBinaryReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2VarCharType, adapter.GetiDB2VarCharReaderMethod, getTrimmedStringWrapper, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.iDB2VarCharBitDataType, adapter.GetiDB2VarCharBitDataReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2VarGraphicType, adapter.GetiDB2VarGraphicReaderMethod, getTrimmedStringWrapper, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.iDB2XmlType, adapter.GetiDB2XmlReaderMethod, dataReaderType: adapter.DataReaderType);

			static Delegate getTimeStampWrapper(Type iDB2TimeStampType)
			{
				//Warning: iDB2TimeStampt produces wrong value for picoseconds when created from DateTime or the milliseconds overload
				//It only works properly when parsing from string or using the picoseconds overload

				//String render and parse variation
				//return ExpressionTools
				//	.FromMethodInvocation<string>(iDB2TimeStampType, "ToNativeFormat")
				//	.Pipe(x => SqlDateTimeParser.ParseDateTime(x))
				//	.Build()
				//	.Compile();

				//Calculation on inner DateTime and Picoseconds variations
				return ExpressionTools
					.FromMemberAccess(iDB2TimeStampType, "Value", "PicoSecond",
						(DateTime value, long picoSecond) => new DateTime(value.Ticks - value.Millisecond * 10000 + picoSecond / 100000))
					.Build()
					.Compile();
			}

			//static Delegate getTrimmedStringWrapper(Type iDB2Type)
			//{
			//	//Call to string on type and then call trim string on the result
			//	return ExpressionTools
			//		.FromMethodInvocation<string>(iDB2Type, "ToString")
			//		.Pipe(ExpressionTools.TrimStringExpression)
			//		.Build()
			//		.Compile();
			//}

			//static Delegate getValue<T>(Type iDB2Type)
			//{
			//	//Call to string on type and then call trim string on the result
			//	return ExpressionTools
			//		.FromMemberAccess<T>(iDB2Type, "Value")
			//		.Build()
			//		.Compile();
			//}
		}
#endif

		private void SetupDB2Connect()
		{
			SqlProviderFlags.IsParameterOrderDependent = false;

			var adapter = (DB2.DB2ProviderAdapter)Adapter.GetInstance();

			//The original implementation had erronously toType and providerType reversed so the following mapping effectivily did nothing
			//Leaving commented for reference in case something needs to be readded
			//In case these are readded they override the generic char trimmings setup with SetCharField

			//SetProviderField(typeof(long), adapter.DB2Int64Type, adapter.GetDB2Int64ReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(int), adapter.DB2Int32Type, adapter.GetDB2Int32ReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(short), adapter.DB2Int16Type, adapter.GetDB2Int16ReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(decimal), adapter.DB2DecimalType, adapter.GetDB2DecimalReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(decimal), adapter.DB2DecimalFloatType, adapter.GetDB2DecimalFloatReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(float), adapter.DB2RealType, adapter.GetDB2RealReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(float), adapter.DB2Real370Type, adapter.GetDB2Real370ReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(double), adapter.DB2DoubleType, adapter.GetDB2DoubleReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.DB2StringType, adapter.GetDB2StringReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.DB2ClobType, adapter.GetDB2ClobReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.DB2BinaryType, adapter.GetDB2BinaryReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.DB2BlobType, adapter.GetDB2BlobReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(DateTime), adapter.DB2DateType, adapter.GetDB2DateReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(TimeSpan), adapter.DB2TimeType, adapter.GetDB2TimeReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(DateTime), adapter.DB2TimeStampType, adapter.GetDB2TimeStampReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(string), adapter.DB2XmlType, adapter.GetDB2XmlReaderMethod, dataReaderType: adapter.DataReaderType);
			//SetProviderField(typeof(byte[]), adapter.DB2RowIdType, adapter.GetDB2RowIdReaderMethod, dataReaderType: adapter.DataReaderType);
		}

		private void SetupOdbc()
		{
			SqlProviderFlags.IsParameterOrderDependent = true;

			//var adapter = (OdbcProviderAdapter)Adapter.GetInstance();
		}

		private void SetupOleDb()
		{
			SqlProviderFlags.IsParameterOrderDependent = true;

			var adapter = (OleDbProviderAdapter)Adapter.GetInstance();

			//Custom mapping from CHAR converted dates and times. Converting datetimes to char needed in connection string.
			SetProviderField<IDataReader, DateTime, string>((r, i) => SqlDateTimeParser.ParseDateTime(r.GetString(i)));
			SetProviderField<IDataReader, DateTimeOffset, string>((r, i) => SqlDateTimeParser.ParseDateTime(r.GetString(i)));
			SetProviderField<IDataReader, TimeSpan, string>((r, i) => SqlDateTimeParser.ParseTimeSpan(r.GetString(i)));
		}

		/// <summary>
		/// This is identical to the base method except that it wraps the method call in a delagate.
		/// </summary>
		protected bool SetProviderField(Type toType, Type fieldType, string methodName, Delegate wrapper, bool throwException = true, Type dataReaderType = null)
		{
			var dataReaderParameter = Expression.Parameter(DataReaderType, "r");
			var indexParameter = Expression.Parameter(typeof(int), "i");

			Expression methodCall;

			if (throwException)
			{
				methodCall = Expression.Call(dataReaderParameter, methodName, null, indexParameter);
			}
			else
			{
				var methodInfo = DataReaderType.GetMethods().FirstOrDefault(m => m.Name == methodName);

				if (methodInfo == null)
					return false;

				methodCall = Expression.Call(dataReaderParameter, methodInfo, indexParameter);
			}

			// wrap the method call in a delegate.
			methodCall = Expression.Invoke(Expression.Constant(wrapper), methodCall);

			if (methodCall.Type != toType)
				methodCall = Expression.Convert(methodCall, toType);

			ReaderExpressions[new ReaderInfo { ToType = toType, ProviderFieldType = fieldType, DataReaderType = dataReaderType }] =
				Expression.Lambda(methodCall, dataReaderParameter, indexParameter);

			return true;
		}

		protected bool SetProviderField(Type toType, Type fieldType, string methodName, Func<Type, Delegate> wrapperFactory, bool throwException = true, Type dataReaderType = null)
			=> SetProviderField(toType, fieldType, methodName, wrapperFactory(toType), throwException, dataReaderType);

		public override TableOptions SupportedTableOptions
		{
			get
			{
				if(this.db2iSeriesSqlProviderFlags.SupportsDropTableIfExists)
				{
					return TableOptions.IsGlobalTemporaryStructure | TableOptions.DropIfExists;
				}

				return TableOptions.IsGlobalTemporaryStructure;
			}
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
				case DataType.DateTime:
					dataType = dataType.WithDataType(DataType.DateTime);

					// iAccessClient fails when passing DateTime objects.
					// Convert them to strings instead.
					if (ProviderType.IsAccessClient() || ProviderType.IsOdbcOrOleDb())
					{
						value = value switch
						{
							DateTime dateTime => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.DateTime, dateTime, false, dataType.Precision),
							DateTimeOffset dateTimeOffset => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.DateTime, dateTimeOffset.DateTime, false, dataType.Precision),
							_ => value
						};
					}

					if(ProviderType.IsOdbcOrOleDb())
					{
						dataType = dataType
							.WithDataType(DataType.VarChar)
							.WithDbType(Constants.DbTypes.TimeStamp);
					}

					break;
				case DataType.Date:

					if (ProviderType.IsAccessClient() || ProviderType.IsOleDb())
					{
						//Date parameters will only accept iDb2Date or string representation of time
						value = value switch
						{
							DateTime dateTime => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Date, dateTime, false),
							DateTimeOffset dateTimeOffset => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Date, dateTimeOffset.DateTime, false),
							_ => value
						};
					}

					if (ProviderType.IsOleDb())
					{
						dataType = dataType
							.WithDataType(DataType.VarChar)
							.WithDbType(Constants.DbTypes.TimeStamp);
					}
					break;
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

			var e = base.GetReaderExpression(reader, idx, readerExpression, toType);
			return e;
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

#if NATIVE_ASYNC
		public override Task<BulkCopyRowsCopied> BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			return bulkCopy.BulkCopyAsync(options.BulkCopyType.GetEffectiveType(), table, options, source, cancellationToken);
		}
#endif
		#endregion
	}
}
