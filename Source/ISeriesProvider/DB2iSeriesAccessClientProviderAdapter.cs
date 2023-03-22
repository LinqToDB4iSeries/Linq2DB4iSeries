#pragma warning disable IDE1006 // Naming Styles to accomodate iDB2 prefix
namespace LinqToDB.DataProvider.DB2iSeries
{
	using System;
	using System.Data;
	using System.Data.Common;
	using System.Linq.Expressions;
	using LinqToDB.Data;
	using LinqToDB.DataProvider;
	using LinqToDB.Expressions;
	using LinqToDB.Mapping;

	internal class DB2iSeriesAccessClientProviderAdapter : IDynamicProviderAdapter
	{
		public const string AssemblyName = "IBM.Data.DB2.iSeries";
		public const string ProviderFactoryName = "IBM.Data.DB2.iSeries";
		public const string ClientNamespace = "IBM.Data.DB2.iSeries";

		private DB2iSeriesAccessClientProviderAdapter()
		{
			var assembly = Common.Tools.TryLoadAssembly(AssemblyName, ProviderFactoryName);
			if (assembly == null)
				throw new InvalidOperationException($"Cannot load assembly {AssemblyName}");

			ConnectionType = assembly.GetType($"{ClientNamespace}.iDB2Connection", true);
			ParameterType = assembly.GetType($"{ClientNamespace}.iDB2Parameter", true);
			DataReaderType = assembly.GetType($"{ClientNamespace}.iDB2DataReader", true);
			TransactionType = assembly.GetType($"{ClientNamespace}.iDB2Transaction", true);
			CommandType = assembly.GetType($"{ClientNamespace}.iDB2Command", true);
			
			var dbType = assembly.GetType($"{ClientNamespace}.iDB2DbType", true);

			MappingSchema = new MappingSchema();

			iDB2BigIntType = loadType("iDB2BigInt", DataType.Int64);
			iDB2BinaryType = loadType("iDB2Binary", DataType.Binary);
			iDB2BlobType = loadType("iDB2Blob", DataType.Blob);
			iDB2CharType = loadType("iDB2Char", DataType.Char);
			iDB2CharBitDataType = loadType("iDB2CharBitData", DataType.Binary);
			iDB2ClobType = loadType("iDB2Clob", DataType.Text);
			iDB2DataLinkType = loadType("iDB2DataLink", DataType.NText);
			iDB2DateType = loadType("iDB2Date", DataType.Date);
			iDB2DbClobType = loadType("iDB2DbClob", DataType.NText);
			iDB2DecFloat16Type = loadType("iDB2DecFloat16", DataType.Decimal);
			iDB2DecFloat34Type = loadType("iDB2DecFloat34", DataType.Decimal);
			iDB2DecimalType = loadType("iDB2Decimal", DataType.Decimal);
			iDB2DoubleType = loadType("iDB2Double", DataType.Double);
			iDB2GraphicType = loadType("iDB2Graphic", DataType.NChar);
			iDB2IntegerType = loadType("iDB2Integer", DataType.Int32);
			iDB2NumericType = loadType("iDB2Numeric", DataType.Decimal);
			iDB2RealType = loadType("iDB2Real", DataType.Single);
			iDB2RowidType = loadType("iDB2Rowid", DataType.VarBinary);
			iDB2SmallIntType = loadType("iDB2SmallInt", DataType.Int16);
			iDB2TimeType = loadType("iDB2Time", DataType.Time);
			iDB2TimeStampType = loadType("iDB2TimeStamp", DataType.Timestamp);
			iDB2VarBinaryType = loadType("iDB2VarBinary", DataType.VarBinary);
			iDB2VarCharType = loadType("iDB2VarChar", DataType.VarChar);
			iDB2VarCharBitDataType = loadType("iDB2VarCharBitData", DataType.VarBinary);
			iDB2VarGraphicType = loadType("iDB2VarGraphic", DataType.NVarChar);
			iDB2XmlType = loadType("iDB2Xml", DataType.Xml);

			var typeMapper = new TypeMapper();

			typeMapper.RegisterTypeWrapper<iDB2Connection>(ConnectionType);
			typeMapper.RegisterTypeWrapper<iDB2Parameter>(ParameterType);
			typeMapper.RegisterTypeWrapper<iDB2DbType>(dbType);

			typeMapper.FinalizeMappings();

			var dbTypeBuilder = typeMapper.Type<iDB2Parameter>().Member(p => p.iDB2DbType);
			SetDbType = dbTypeBuilder.BuildSetter<IDbDataParameter>();
			GetDbType = dbTypeBuilder.BuildGetter<IDbDataParameter>();

			
			CreateConnection = typeMapper.BuildWrappedFactory((string connectionString) => new iDB2Connection(connectionString));
			GetLibraryList = typeMapper.BuildFunc<DbConnection, string>(typeMapper.MapLambda((iDB2Connection conn) => conn.LibraryList));
			GetNamingConvention = typeMapper.BuildFunc<DbConnection, iDB2NamingConvention>(typeMapper.MapLambda((iDB2Connection conn) => conn.Naming));

			DeriveParameters = buildActionInvoker(CommandType, "DeriveParameters");
			AddBatch = buildActionInvoker(CommandType, "AddBatch");

			DataConnection.WriteTraceLine(assembly.FullName, nameof(DB2iSeriesAccessClientProviderAdapter), System.Diagnostics.TraceLevel.Info);

			static Action<DbCommand> buildActionInvoker(Type type, string methodName)
			{
				var method = type.GetMethod(methodName);
				var cmdParameter = Expression.Parameter(typeof(DbCommand), "cmd");
				var expression = Expression.Call(Expression.Convert(cmdParameter, type), method);
				var lambda = Expression.Lambda<Action<DbCommand>>(expression, cmdParameter);
				return lambda.Compile();
			}

			Type loadType(string typeName, DataType dataType)
			{
				var type = assembly.GetType($"{ClientNamespace}.{typeName}", true);

				var getNullValue = Expression.Lambda<Func<object>>(Expression.Convert(ExpressionHelper.Field(type, "Null"), typeof(object))).Compile();
				MappingSchema.AddScalarType(type, getNullValue(), true, dataType);

				return type;
			}
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

		public Action<DbParameter, iDB2DbType> SetDbType { get; }
		public Func<DbParameter, iDB2DbType> GetDbType { get; }

		public Func<string, iDB2Connection> CreateConnection { get; }

		public Func<DbConnection, string> GetLibraryList { get; }
		public Func<DbConnection, iDB2NamingConvention> GetNamingConvention { get; }

		public Action<DbCommand> DeriveParameters;
		public Action<DbCommand> AddBatch;

		private static Lazy<DB2iSeriesAccessClientProviderAdapter> lazyInstance = new(() => new());
		public static DB2iSeriesAccessClientProviderAdapter Instance => lazyInstance.Value;
		
		public DataType GetDataType(iDB2DbType dbType)
		{
			return dbType switch
			{
				iDB2DbType.iDB2BigInt => DataType.Int64,
				iDB2DbType.iDB2Binary => DataType.Binary,
				iDB2DbType.iDB2Blob => DataType.Blob,
				iDB2DbType.iDB2Char => DataType.Char,
				iDB2DbType.iDB2CharBitData => DataType.Binary,
				iDB2DbType.iDB2Clob => DataType.Text,
				iDB2DbType.iDB2DataLink => DataType.Binary,
				iDB2DbType.iDB2Date => DataType.Date,
				iDB2DbType.iDB2DbClob => DataType.NText,
				iDB2DbType.iDB2DecFloat16 => DataType.Decimal,
				iDB2DbType.iDB2DecFloat34 => DataType.Decimal,
				iDB2DbType.iDB2Decimal => DataType.Decimal,
				iDB2DbType.iDB2Double => DataType.Double,
				iDB2DbType.iDB2Graphic => DataType.NVarChar,
				iDB2DbType.iDB2Integer => DataType.Int32,
				iDB2DbType.iDB2Numeric => DataType.Decimal,
				iDB2DbType.iDB2Real => DataType.Single,
				iDB2DbType.iDB2Rowid => DataType.Binary,
				iDB2DbType.iDB2SmallInt => DataType.Int16,
				iDB2DbType.iDB2Time => DataType.Time,
				iDB2DbType.iDB2TimeStamp => DataType.Timestamp,
				iDB2DbType.iDB2VarBinary => DataType.VarBinary,
				iDB2DbType.iDB2VarChar => DataType.VarChar,
				iDB2DbType.iDB2VarCharBitData => DataType.VarBinary,
				iDB2DbType.iDB2VarGraphic => DataType.NVarChar,
				iDB2DbType.iDB2Xml => DataType.Xml,
				_ => DataType.Undefined,
			};
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
				(Expression<Func<iDB2Connection, DbCommand>>)((iDB2Connection this_) => this_.CreateCommand()),
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

			public DbCommand CreateCommand() => ((Func<iDB2Connection, DbCommand>)CompiledWrappers[2])(this);
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
