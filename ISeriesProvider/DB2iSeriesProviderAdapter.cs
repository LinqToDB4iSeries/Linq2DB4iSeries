#pragma warning disable IDE1006 // Naming Styles to accomodate iDB2 prefix
namespace LinqToDB.DataProvider.DB2iSeries
{
	using System;
	using System.Data;
	using System.Linq.Expressions;
	using LinqToDB.Data;
	using LinqToDB.DataProvider;
	using LinqToDB.Expressions;
	using LinqToDB.Mapping;

	public class DB2iSeriesProviderAdapter : IDynamicProviderAdapter
	{
		public const string AssemblyName = "IBM.Data.DB2.iSeries";
		public const string ProviderFactoryName = "IBM.Data.DB2.iSeries";
		public const string ClientNamespace = "IBM.Data.DB2.iSeries";

		private static readonly object _syncRoot = new object();
		private static DB2iSeriesProviderAdapter _instance;

		private DB2iSeriesProviderAdapter(
			Type connectionType,
			Type dataReaderType,
			Type parameterType,
			Type commandType,
			Type transactionType,

			Type iDB2BigIntType,
			Type iDB2BinaryType,
			Type iDB2BlobType,
			Type iDB2CharType,
			Type iDB2CharBitDataType,
			Type iDB2ClobType,
			Type iDB2DataLinkType,
			Type iDB2DateType,
			Type iDB2DbClobType,
			Type iDB2DecFloat16Type,
			Type iDB2DecFloat34Type,
			Type iDB2DecimalType,
			Type iDB2DoubleType,
			Type iDB2GraphicType,
			Type iDB2IntegerType,
			Type iDB2NumericType,
			Type iDB2RealType,
			Type iDB2RowidType,
			Type iDB2SmallIntType,
			Type iDB2TimeType,
			Type iDB2TimeStampType,
			Type iDB2VarBinaryType,
			Type iDB2VarCharType,
			Type iDB2VarCharBitDataType,
			Type iDB2VarGraphicType,
			Type iDB2XmlType,

			MappingSchema mappingSchema,

			Action<IDbDataParameter, iDB2DbType> dbTypeSetter,
			Func<IDbDataParameter, iDB2DbType> dbTypeGetter,

		Func<string, iDB2Connection> connectionCreator,
		Func<IDbConnection, string> libraryListGetter,
		Func<IDbConnection, iDB2NamingConvention> namingGetter)
		{
			ConnectionType = connectionType;
			DataReaderType = dataReaderType;
			ParameterType = parameterType;
			CommandType = commandType;
			TransactionType = transactionType;

			this.iDB2BigIntType = iDB2BigIntType;
			this.iDB2BinaryType = iDB2BinaryType;
			this.iDB2BlobType = iDB2BlobType;
			this.iDB2CharType = iDB2CharType;
			this.iDB2CharBitDataType = iDB2CharBitDataType;
			this.iDB2ClobType = iDB2ClobType;
			this.iDB2DataLinkType = iDB2DataLinkType;
			this.iDB2DateType = iDB2DateType;
			this.iDB2DbClobType = iDB2DbClobType;
			this.iDB2DecFloat16Type = iDB2DecFloat16Type;
			this.iDB2DecFloat34Type = iDB2DecFloat34Type;
			this.iDB2DecimalType = iDB2DecimalType;
			this.iDB2DoubleType = iDB2DoubleType;
			this.iDB2GraphicType = iDB2GraphicType;
			this.iDB2IntegerType = iDB2IntegerType;
			this.iDB2NumericType = iDB2NumericType;
			this.iDB2RealType = iDB2RealType;
			this.iDB2RowidType = iDB2RowidType;
			this.iDB2SmallIntType = iDB2SmallIntType;
			this.iDB2TimeType = iDB2TimeType;
			this.iDB2TimeStampType = iDB2TimeStampType;
			this.iDB2VarBinaryType = iDB2VarBinaryType;
			this.iDB2VarCharType = iDB2VarCharType;
			this.iDB2VarCharBitDataType = iDB2VarCharBitDataType;
			this.iDB2VarGraphicType = iDB2VarGraphicType;
			this.iDB2XmlType = iDB2XmlType;

			MappingSchema = mappingSchema;

			SetDbType = dbTypeSetter;
			GetDbType = dbTypeGetter;

			CreateConnection = connectionCreator;
			GetLibraryList = libraryListGetter;
			GetNamingConvention = namingGetter;
		}

		public Type ConnectionType { get; }
		public Type DataReaderType { get; }
		public Type ParameterType { get; }
		public Type CommandType { get; }
		public Type TransactionType { get; }

		public MappingSchema MappingSchema { get; }

		public Type iDB2BigIntType { get; }
		public Type iDB2BinaryType { get; }
		public Type iDB2BlobType { get; }
		public Type iDB2CharType { get; }
		public Type iDB2CharBitDataType { get; }
		public Type iDB2ClobType { get; }
		public Type iDB2DataLinkType { get; }
		public Type iDB2DateType { get; }
		public Type iDB2DbClobType { get; }
		public Type iDB2DecFloat16Type { get; }
		public Type iDB2DecFloat34Type { get; }
		public Type iDB2DecimalType { get; }
		public Type iDB2DoubleType { get; }
		public Type iDB2GraphicType { get; }
		public Type iDB2IntegerType { get; }
		public Type iDB2NumericType { get; }
		public Type iDB2RealType { get; }
		public Type iDB2RowidType { get; }
		public Type iDB2SmallIntType { get; }
		public Type iDB2TimeType { get; }
		public Type iDB2TimeStampType { get; }
		public Type iDB2VarBinaryType { get; }
		public Type iDB2VarCharType { get; }
		public Type iDB2VarCharBitDataType { get; }
		public Type iDB2VarGraphicType { get; }
		public Type iDB2XmlType { get; }

		public string GetiDB2BigIntReaderMethod => "GetiDB2BigInt";
		public string GetiDB2BinaryReaderMethod => "GetiDB2Binary";
		public string GetiDB2BlobReaderMethod => "GetiDB2Blob";
		public string GetiDB2CharReaderMethod => "GetiDB2Char";
		public string GetiDB2CharBitDataReaderMethod => "GetiDB2CharBitData";
		public string GetiDB2ClobReaderMethod => "GetiDB2Clob";
		public string GetiDB2DataLinkReaderMethod => "GetiDB2DataLink";
		public string GetiDB2DateReaderMethod => "GetiDB2Date";
		public string GetiDB2DbClobReaderMethod => "GetiDB2DbClob";
		public string GetiDB2DecFloat16ReaderMethod => "GetiDB2DecFloat16";
		public string GetiDB2DecFloat34ReaderMethod => "GetiDB2DecFloat34";
		public string GetiDB2DecimalReaderMethod => "GetiDB2Decimal";
		public string GetiDB2DoubleReaderMethod => "GetiDB2Double";
		public string GetiDB2GraphicReaderMethod => "GetiDB2Graphic";
		public string GetiDB2IntegerReaderMethod => "GetiDB2Integer";
		public string GetiDB2NumericReaderMethod => "GetiDB2Numeric";
		public string GetiDB2RealReaderMethod => "GetiDB2Real";
		public string GetiDB2RowidReaderMethod => "GetiDB2Rowid";
		public string GetiDB2SmallIntReaderMethod => "GetiDB2SmallInt";
		public string GetiDB2TimeReaderMethod => "GetiDB2Time";
		public string GetiDB2TimeStampReaderMethod => "GetiDB2TimeStamp";
		public string GetiDB2VarBinaryReaderMethod => "GetiDB2VarBinary";
		public string GetiDB2VarCharReaderMethod => "GetiDB2VarChar";
		public string GetiDB2VarCharBitDataReaderMethod => "GetiDB2VarCharBitData";
		public string GetiDB2VarGraphicReaderMethod => "GetiDB2VarGraphic";
		public string GetiDB2XmlReaderMethod => "GetiDB2Xml";

		public Action<IDbDataParameter, iDB2DbType> SetDbType { get; }
		public Func<IDbDataParameter, iDB2DbType> GetDbType { get; }

		public Func<string, iDB2Connection> CreateConnection { get; }

		public Func<IDbConnection, string> GetLibraryList { get; }
		public Func<IDbConnection, iDB2NamingConvention> GetNamingConvention { get; }

		public static DB2iSeriesProviderAdapter GetInstance()
		{
			if (_instance == null)
				lock (_syncRoot)
					if (_instance == null)
					{
						var assembly = Common.Tools.TryLoadAssembly(AssemblyName, ProviderFactoryName);
						if (assembly == null)
							throw new InvalidOperationException($"Cannot load assembly {AssemblyName}");

						var connectionType = assembly.GetType($"{ClientNamespace}.iDB2Connection", true);
						var parameterType = assembly.GetType($"{ClientNamespace}.iDB2Parameter", true);
						var dataReaderType = assembly.GetType($"{ClientNamespace}.iDB2DataReader", true);
						var transactionType = assembly.GetType($"{ClientNamespace}.iDB2Transaction", true);
						var commandType = assembly.GetType($"{ClientNamespace}.iDB2Command", true);
						var dbType = assembly.GetType($"{ClientNamespace}.iDB2DbType", true);

						var mappingSchema = new MappingSchema();

						var iDB2BigIntType = loadType("iDB2BigInt", DataType.Int64);
						var iDB2BinaryType = loadType("iDB2Binary", DataType.Binary);
						var iDB2BlobType = loadType("iDB2Blob", DataType.Blob);
						var iDB2CharType = loadType("iDB2Char", DataType.Char);
						var iDB2CharBitDataType = loadType("iDB2CharBitData", DataType.Binary);
						var iDB2ClobType = loadType("iDB2Clob", DataType.Text);
						var iDB2DataLinkType = loadType("iDB2DataLink", DataType.NText);
						var iDB2DateType = loadType("iDB2Date", DataType.Date);
						var iDB2DbClobType = loadType("iDB2DbClob", DataType.NText);
						var iDB2DecFloat16Type = loadType("iDB2DecFloat16", DataType.Decimal);
						var iDB2DecFloat34Type = loadType("iDB2DecFloat34", DataType.Decimal);
						var iDB2DecimalType = loadType("iDB2Decimal", DataType.Decimal);
						var iDB2DoubleType = loadType("iDB2Double", DataType.Double);
						var iDB2GraphicType = loadType("iDB2Graphic", DataType.NChar);
						var iDB2IntegerType = loadType("iDB2Integer", DataType.Int32);
						var iDB2NumericType = loadType("iDB2Numeric", DataType.Decimal);
						var iDB2RealType = loadType("iDB2Real", DataType.Single);
						var iDB2RowidType = loadType("iDB2Rowid", DataType.VarBinary);
						var iDB2SmallIntType = loadType("iDB2SmallInt", DataType.Int16);
						var iDB2TimeType = loadType("iDB2Time", DataType.Time);
						var iDB2TimeStampType = loadType("iDB2TimeStamp", DataType.DateTime2);
						var iDB2VarBinaryType = loadType("iDB2VarBinary", DataType.VarBinary);
						var iDB2VarCharType = loadType("iDB2VarChar", DataType.VarChar);
						var iDB2VarCharBitDataType = loadType("iDB2VarCharBitData", DataType.VarBinary);
						var iDB2VarGraphicType = loadType("iDB2VarGraphic", DataType.NVarChar);
						var iDB2XmlType = loadType("iDB2Xml", DataType.Xml);

						var typeMapper = new TypeMapper();

						typeMapper.RegisterTypeWrapper<iDB2Connection>(connectionType);
						typeMapper.RegisterTypeWrapper<iDB2Parameter>(parameterType);
						typeMapper.RegisterTypeWrapper<iDB2DbType>(dbType);

						typeMapper.FinalizeMappings();

						var dbTypeBuilder = typeMapper.Type<iDB2Parameter>().Member(p => p.iDB2DbType);
						var typeSetter = dbTypeBuilder.BuildSetter<IDbDataParameter>();
						var typeGetter = dbTypeBuilder.BuildGetter<IDbDataParameter>();

						_instance = new DB2iSeriesProviderAdapter(
							connectionType,
							dataReaderType,
							parameterType,
							commandType,
							transactionType,

							iDB2BigIntType,
							iDB2BinaryType,
							iDB2BlobType,
							iDB2CharType,
							iDB2CharBitDataType,
							iDB2ClobType,
							iDB2DataLinkType,
							iDB2DateType,
							iDB2DbClobType,
							iDB2DecFloat16Type,
							iDB2DecFloat34Type,
							iDB2DecimalType,
							iDB2DoubleType,
							iDB2GraphicType,
							iDB2IntegerType,
							iDB2NumericType,
							iDB2RealType,
							iDB2RowidType,
							iDB2SmallIntType,
							iDB2TimeType,
							iDB2TimeStampType,
							iDB2VarBinaryType,
							iDB2VarCharType,
							iDB2VarCharBitDataType,
							iDB2VarGraphicType,
							iDB2XmlType,

							mappingSchema,

							typeSetter,
							typeGetter,
							typeMapper.BuildWrappedFactory((string connectionString) => new iDB2Connection(connectionString)),
							typeMapper.BuildFunc<IDbConnection, string>(typeMapper.MapLambda((iDB2Connection conn) => conn.LibraryList)),
							typeMapper.BuildFunc<IDbConnection, iDB2NamingConvention>(typeMapper.MapLambda((iDB2Connection conn) => conn.Naming))
						);

						DataConnection.WriteTraceLine(assembly.FullName, nameof(DB2iSeriesProviderAdapter), System.Diagnostics.TraceLevel.Info);

						Type loadType(string typeName, DataType dataType)
						{
							var type = assembly.GetType($"{ClientNamespace}.{typeName}", true);

							var getNullValue = Expression.Lambda<Func<object>>(Expression.Convert(ExpressionHelper.Field(type, "Null"), typeof(object))).Compile();
							mappingSchema.AddScalarType(type, getNullValue(), true, dataType);

							return type;
						}
					}

			return _instance;
		}

		#region Wrappers

		[Wrapper]
		public class iDB2Connection : TypeWrapper, IDisposable
		{
			private static LambdaExpression[] Wrappers { get; }
				= new LambdaExpression[]
			{
				// [0]: get ServerVersion
				(Expression<Func<iDB2Connection, string>>)((iDB2Connection this_) => this_.ServerVersion),
				// [1]: get LibraryList
				(Expression<Func<iDB2Connection, string>>)((iDB2Connection this_) => this_.LibraryList),
				// [2]: CreateCommand
				(Expression<Func<iDB2Connection, IDbCommand>>)((iDB2Connection this_) => this_.CreateCommand()),
				// [3]: Open
				(Expression<Action<iDB2Connection>>)((iDB2Connection this_) => this_.Open()),
				// [4]: Dispose
				(Expression<Action<iDB2Connection>>)((iDB2Connection this_) => this_.Dispose()),
				// [5]: Naming
				(Expression<Func<iDB2Connection, iDB2NamingConvention>>)((iDB2Connection this_) => this_.Naming),
			};

			public iDB2Connection(object instance, Delegate[] wrappers) : base(instance, wrappers)
			{
			}

			public iDB2Connection(string connectionString) => throw new NotImplementedException();

			public string ServerVersion => ((Func<iDB2Connection, string>)CompiledWrappers[0])(this);
			public string LibraryList => ((Func<iDB2Connection, string>)CompiledWrappers[1])(this);

			public IDbCommand CreateCommand() => ((Func<iDB2Connection, IDbCommand>)CompiledWrappers[2])(this);
			public void Open() => ((Action<iDB2Connection>)CompiledWrappers[3])(this);
			public void Dispose() => ((Action<iDB2Connection>)CompiledWrappers[4])(this);
			public iDB2NamingConvention Naming => ((Func<iDB2Connection, iDB2NamingConvention>)CompiledWrappers[5])(this);
		}

		[Wrapper]
		private class iDB2Parameter
		{
			public iDB2DbType iDB2DbType { get; set; }
		}

		[Wrapper]
		public enum iDB2DbType
		{
			iDB2BigInt = 1,
			iDB2Binary = 18,
			iDB2Blob = 20,
			iDB2Char = 6,
			iDB2CharBitData = 8,
			iDB2Clob = 21,
			iDB2DataLink = 23,
			iDB2Date = 12,
			iDB2DbClob = 22,
			iDB2DecFloat16 = 24,
			iDB2DecFloat34 = 25,
			iDB2Decimal = 4,
			iDB2Double = 17,
			iDB2Graphic = 10,
			iDB2Integer = 2,
			iDB2Numeric = 5,
			iDB2Real = 16,
			iDB2Rowid = 15,
			iDB2SmallInt = 3,
			iDB2Time = 13,
			iDB2TimeStamp = 14,
			iDB2VarBinary = 19,
			iDB2VarChar = 7,
			iDB2VarCharBitData = 9,
			iDB2VarGraphic = 11,
			iDB2Xml = 26
		}

		[Wrapper]
		public enum iDB2NamingConvention
		{
			SQL = 0,
			System = 1
		}

		#endregion
	}
}
#pragma warning restore IDE1006 // Naming Styles
