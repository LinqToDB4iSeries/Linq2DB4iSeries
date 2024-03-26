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
	using System.Data.Common;
#if NETFRAMEWORK
	using static DB2iSeriesAccessClientProviderAdapter;
#endif

	public class DB2iSeriesDataProvider : DynamicDataProviderBase<DB2iSeriesProviderAdapter>
	{
		public DB2iSeriesProviderType ProviderType { get; }

		private readonly DB2iSeriesSchemaProvider schemaProvider;
		private readonly DB2iSeriesBulkCopy bulkCopy;
		private readonly DB2iSeriesSqlProviderFlags db2iSeriesSqlProviderFlags;
		private readonly DB2iSeriesMappingOptions mappingOptions;

		/// <summary>
		/// Build a DB2 iSeries data provider.
		/// </summary>
		/// <param name="providerOptions">The provider's construction options, see <see cref="DB2iSeriesProviderOptions"/></param>
		internal protected DB2iSeriesDataProvider(DB2iSeriesProviderOptions providerOptions)
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
			SqlProviderFlags.IsExistsPreferableForContains = true;
			//This feature is undocumented, it passes the tests on 7.5
			//DB2 supports Comparison, Between, Update, UpateLiteral these don't work in iDB2
			SqlProviderFlags.RowConstructorSupport = RowFeature.Equality | RowFeature.Overlaps;

			db2iSeriesSqlProviderFlags.SetCustomFlags(SqlProviderFlags);
			mappingOptions.SetCustomFlags(SqlProviderFlags);

			SetCharFieldToType<char>(Constants.DbTypes.Char, DataTools.GetCharExpression);
			SetCharField(Constants.DbTypes.Char, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.NChar, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.Graphic, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.VarChar, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.NVarChar, ReaderExpressionTools.GetTrimmedStringExpression);
			SetCharField(Constants.DbTypes.VarGraphic, ReaderExpressionTools.GetTrimmedStringExpression);

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

			SetProviderField(typeof(DateTime), adapter.iDB2TimeStampType, adapter.GetiDB2TimeStampReaderMethod, getTimeStampWrapper(adapter.iDB2TimeStampType), dataReaderType: adapter.DataReaderType);
			SetProviderField(typeof(DateTimeOffset), adapter.iDB2TimeStampType, adapter.GetiDB2TimeStampReaderMethod, getTimeStampWrapper(adapter.iDB2TimeStampType), dataReaderType: adapter.DataReaderType);
			
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
		}

		private void SetupOdbc()
		{
			SqlProviderFlags.IsParameterOrderDependent = true;
		}

		private void SetupOleDb()
		{
			SqlProviderFlags.IsParameterOrderDependent = true;

			var adapter = (OleDbProviderAdapter)Adapter.GetInstance();

			//Custom mapping from CHAR converted dates and times. Converting datetimes to char needed in connection string.
			SetProviderField<DbDataReader, DateTime, string>((r, i) => SqlDateTimeParser.ParseDateTime(r.GetString(i)));
			SetProviderField<DbDataReader, DateTimeOffset, string>((r, i) => SqlDateTimeParser.ParseDateTime(r.GetString(i)));
			SetProviderField<DbDataReader, TimeSpan, string>((r, i) => SqlDateTimeParser.ParseTimeSpan(r.GetString(i)));
		}
		
		public override DbCommand InitCommand(DataConnection dataConnection, DbCommand command, CommandType commandType, string commandText, DataParameter[] parameters, bool withParameters)
		{
			//Handle ODBC stored procedure when only proc name is provided
			if (ProviderType == DB2iSeriesProviderType.Odbc
				&& commandType == CommandType.StoredProcedure
				&& !commandText.Contains(' ') //single word - presumed proc name
			)
			{
				var builder = this.CreateDB2iSeriesSqlBuilder(this.MappingSchema, dataConnection.Options);
				commandText = $"{{{builder.BuildStoredProcedureCall(commandText, parameters)}}}";
			}

			return base.InitCommand(dataConnection, command, commandType, commandText, parameters, withParameters);
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
				if (this.db2iSeriesSqlProviderFlags.SupportsDropTableIfExists)
				{
					return TableOptions.IsGlobalTemporaryStructure | TableOptions.DropIfExists;
				}

				return TableOptions.IsGlobalTemporaryStructure;
			}
		}

		public override ISqlBuilder CreateSqlBuilder(MappingSchema mappingSchema, DataOptions dataOptions)
		{
			return CreateDB2iSeriesSqlBuilder(mappingSchema, dataOptions);
		}

		private DB2iSeriesSqlBuilder CreateDB2iSeriesSqlBuilder(MappingSchema mappingSchema, DataOptions dataOptions)
		{
			return new DB2iSeriesSqlBuilder(this, mappingSchema, dataOptions, GetSqlOptimizer(dataOptions), SqlProviderFlags, db2iSeriesSqlProviderFlags);
		}

		public override ISchemaProvider GetSchemaProvider()
		{
			return schemaProvider;
		}

		public override ISqlOptimizer GetSqlOptimizer(DataOptions dataOptions)
		{
			return new DB2iSeriesSqlOptimizer(SqlProviderFlags, db2iSeriesSqlProviderFlags, dataOptions); 
		}

		private static MappingSchema GetMappingSchema(string configuration, DB2iSeriesProviderType providerType, bool mapGuidAsString)
		{
			var providerSchema = providerType switch
			{
#if NETFRAMEWORK
				DB2iSeriesProviderType.AccessClient => DB2iSeriesAccessClientProviderAdapter.Instance.MappingSchema,
#endif
				DB2iSeriesProviderType.DB2 => DB2.DB2ProviderAdapter.Instance.MappingSchema,
				_ => new MappingSchema()
			};

			return mapGuidAsString
				? new DB2iSeriesGuidAsStringMappingSchema(configuration, providerSchema)
				: new DB2iSeriesMappingSchema(configuration, providerSchema);
		}

		public override void SetParameter(DataConnection dataConnection, DbParameter parameter, string name, DbDataType dataType, object value)
		{
			if (value is not DBNull && value is not null)
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

					//Sanitize DataType based on provided DbType
					dataType = dataType.WithDataType(dataType.DbType?.ToUpper() switch
					{
						Constants.DbTypes.Date => DataType.Date,
						Constants.DbTypes.Time => DataType.Time,
						_ => DataType.DateTime
					});

					//Sanitize DbType based on DataType if DbType not provided
					//if (dataType.DbType is null)
					//{
					//	dataType = dataType.WithDbType(dataType.DataType switch
					//	{
					//		DataType.Date => Constants.DbTypes.Date,
					//		DataType.Time => Constants.DbTypes.Time,
					//		_ => Constants.DbTypes.TimeStamp
					//	});
					//}

					// iAccessClient and OleDb fail when passing DateTime objects.
					// ODbc works with precision up to 3
					// Convert them to strings instead.
					if (ProviderType.IsAccessClient() || ProviderType.IsOdbcOrOleDb())
					{
						value = value switch
						{
							DateTime dateTime => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(dataType.DataType, dateTime, false, dataType.Precision),
							DateTimeOffset dateTimeOffset => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(dataType.DataType, dateTimeOffset.DateTime, false, dataType.Precision),
							_ => value
						};


							if (ProviderType.IsOdbcOrOleDb()
								&& dataType.DataType == DataType.DateTime)
							{
								//Treat source data as string to be converted to datetime
								//Otherwise odbc truncates precision to 3 and oledb fails with overflow
								//Access client converts it accurately	
								dataType = dataType
									.WithDataType(DataType.VarChar);
							}
						}

						break;
					case DataType.Date:

#if NET6_0_OR_GREATER
						if (value is DateOnly d)
							value = d.ToDateTime(TimeOnly.MinValue);
#endif
						//if (dataType.DbType != null)
						//	dataType = dataType
						//		.WithDbType(Constants.DbTypes.Date);

						if (ProviderType.IsAccessClient() || ProviderType.IsOleDb())
						{
							//Date parameters will only accept iDb2Date or string representation of time
							value = value switch
							{
								DateTime dateTime => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(dataType.DataType, dateTime, false),
								DateTimeOffset dateTimeOffset => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(dataType.DataType, dateTimeOffset.DateTime, false),
								_ => value
							};
						}

						break;
					case DataType.Time:

						//if (dataType.DbType != null)
						//	dataType = dataType
						//		.WithDbType(Constants.DbTypes.Time);

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
			}

			base.SetParameter(dataConnection, parameter, name, dataType, value);
		}

		private object GetParameterProviderType(DataType dataType)
		{
			return ProviderType switch
			{
#if NETFRAMEWORK
				DB2iSeriesProviderType.AccessClient => dataType switch
				{
					DataType.Blob => iDB2DbType.iDB2Blob,
					DataType.DateTime2 => iDB2DbType.iDB2TimeStamp,
					DataType.VarNumeric => iDB2DbType.iDB2Decimal,
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
					DataType.Date => OdbcType.Date,
					DataType.Time => OdbcType.Time,
					DataType.Guid => OdbcType.VarBinary,
					DataType.SByte => OdbcType.SmallInt,
					DataType.UInt16 => OdbcType.Int,
					DataType.UInt32 => OdbcType.BigInt,
					DataType.UInt64 => OdbcType.Decimal,
					DataType.VarNumeric => OdbcType.Decimal,
					var x when
						x == DataType.DateTime
					|| x == DataType.DateTime2 => OdbcType.DateTime,
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
					|| x == DataType.SByte
					|| x == DataType.Boolean => OleDbType.SmallInt,
					var x when
						x == DataType.UInt64
					||  x == DataType.VarNumeric
					||	x == DataType.Decimal => OleDbType.Decimal,
					var x when
						x == DataType.DateTime
					|| x == DataType.DateTime2 => OleDbType.DBTimeStamp,
					_ => null
				},
				_ => throw ExceptionHelper.InvalidAdoProvider(ProviderType)
			};
		}

		private void SetParameterDbType(DataType dataType, DbParameter parameter)
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

		protected override void SetParameterType(DataConnection dataConnection, DbParameter parameter, DbDataType dataType)
		{
			var type = GetParameterProviderType(dataType.DataType);
			
			if (type != null)
			{
				var param = TryGetProviderParameter(dataConnection, parameter);
				if (param != null)
				{
					Adapter.SetDbType(param, type);
					return;
				}
			}

			SetParameterDbType(dataType.DataType, parameter);

			base.SetParameterType(dataConnection, parameter, dataType);
		}

		public bool TryGetProviderParameterName(IDataContext context, DbParameter parameter, out string name)
		{
			var param = TryGetProviderParameter(context, parameter);
			if (param != null)
			{
				name = Adapter.GetDbTypeName(param);
				return true;
			}

			name = null;
			return false;
		}

		public bool TryGetProviderConnection(DataConnection dataConnection, out DbConnection providerConnection)
		{
			providerConnection = TryGetProviderConnection(dataConnection, dataConnection.Connection);
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

		public override Expression GetReaderExpression(DbDataReader reader, int idx, Expression readerExpression, Type toType)
		{
			//Wrap OdbcDataReader to avoid exceptions on XML columns
			if (ProviderType.IsOdbc())
			{
				reader = reader is DbDataReader odbcDataReader
					&& reader.GetType().Name == "OdbcDataReader" ?
				new OdbcDataReaderWrapper(odbcDataReader) : reader;
			}

			var e = base.GetReaderExpression(reader, idx, readerExpression, toType);
			return e;
		}
		
		public override bool? IsDBNullAllowed(DataOptions dataOptions, DbDataReader reader, int idx)
		{
			//Always return true on ODBC to avoid exceptions on XML columns
			if (ProviderType.IsOdbc())
				return true;

			return base.IsDBNullAllowed(dataOptions, reader, idx);
		}

		#region BulkCopy

		
		public override BulkCopyRowsCopied BulkCopy<T>(DataOptions options, ITable<T> table, IEnumerable<T> source)
		{
			return bulkCopy.BulkCopy(options.BulkCopyOptions.BulkCopyType.GetEffectiveType(), table, options, source);
		}

		public override Task<BulkCopyRowsCopied> BulkCopyAsync<T>(DataOptions options, ITable<T> table, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			return bulkCopy.BulkCopyAsync(options.BulkCopyOptions.BulkCopyType.GetEffectiveType(), table, options, source, cancellationToken);
		}

#if !NET45
		public override Task<BulkCopyRowsCopied> BulkCopyAsync<T>(DataOptions options, ITable<T> table, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			return bulkCopy.BulkCopyAsync(options.BulkCopyOptions.BulkCopyType.GetEffectiveType(), table, options, source, cancellationToken);
		}
#endif
		#endregion
	}
}
