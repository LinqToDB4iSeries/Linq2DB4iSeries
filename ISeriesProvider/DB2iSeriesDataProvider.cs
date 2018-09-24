using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using Data;
    using Extensions;
    using Mapping;
    using SchemaProvider;
    using SqlProvider;

    public class DB2iSeriesDataProvider : DynamicDataProviderBase
    {
        #region Private fields / Public Properties

        private readonly DB2iSeriesDataProviderOptions options;
        readonly DB2iSeriesSqlOptimizer _sqlOptimizer;
        Action<IDbDataParameter> _setBlob; //Removed static, needs different implementation per providerType
        DB2iSeriesBulkCopy _bulkCopy;

        public string DummyTableName => DB2iSeriesTools.iSeriesDummyTableName();

        #endregion

        #region Constructor

        public DB2iSeriesDataProvider() : this(DB2iSeriesProviderName.DB2iSeries_AccessClient)
        {
        }

        public DB2iSeriesDataProvider(DB2iSeriesAdoProviderType adoProviderType, DB2iSeriesLevels minLevel, bool mapGuidAsString)
            :this(new DB2iSeriesDataProviderOptions(minLevel, mapGuidAsString, adoProviderType))
        {

        }

        public DB2iSeriesDataProvider(DB2iSeriesDataProviderOptions options)
            : this(DB2iSeriesProviderName.GetFromOptions(options))
        {
            
        }

        public DB2iSeriesDataProvider(string name) : base(name, null)
        {
            if (!DB2iSeriesProviderName.AllNames.Contains(name))
                throw new NotSupportedException($"Invalid provider name {name}. Valid options are: " + Environment.NewLine + string.Join(Environment.NewLine, DB2iSeriesProviderName.AllNames));

            options = DB2iSeriesProviderName.GetOptions(name);

            LoadExpressions(name, options.MapGuidAsString);

            SqlProviderFlags.AcceptsTakeAsParameter = false;
            SqlProviderFlags.AcceptsTakeAsParameterIfSkip = true;
            SqlProviderFlags.IsDistinctOrderBySupported = false;
            SqlProviderFlags.CanCombineParameters = false;
            SqlProviderFlags.IsParameterOrderDependent = true;
            SqlProviderFlags.IsCommonTableExpressionsSupported = true;

            if (options.MapGuidAsString)
                SqlProviderFlags.CustomFlags.Add(nameof(DB2iSeriesDataProviderOptions.MapGuidAsString));

            SetCharField("CHAR", (r, i) => r.GetString(i).TrimEnd(' '));
            SetCharField("NCHAR", (r, i) => r.GetString(i).TrimEnd(' '));

            _sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags);
        }

        
        //Obsolete - name and options are linked cannot mix and match 
        //public DB2iSeriesDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString) : base(name, null)
        //{
            
        //}

        #endregion

        #region DataProvider Initialization

        private void OnConnectionTypeCreated_AccessClient(Type connectionType)
        {
            DB2iSeriesTypes.ConnectionType = connectionType;

            var assembly = connectionType.Assembly;
            Type getType(string typeName) => assembly.GetType($"{DB2iSeriesTools.AssemblyName_AccessClient}.{typeName}", true);

            DB2iSeriesTypes.BigInt.Type = getType("iDB2BigInt");
            DB2iSeriesTypes.Binary.Type = getType("iDB2Binary");
            DB2iSeriesTypes.Blob.Type = getType("iDB2Blob");
            DB2iSeriesTypes.Char.Type = getType("iDB2Char");
            DB2iSeriesTypes.CharBitData.Type = getType("iDB2CharBitData");
            DB2iSeriesTypes.Clob.Type = getType("iDB2Clob");
            DB2iSeriesTypes.Date.Type = getType("iDB2Date");
            DB2iSeriesTypes.DataLink.Type = getType("iDB2DataLink");
            DB2iSeriesTypes.DbClob.Type = getType("iDB2DbClob");
            DB2iSeriesTypes.DecFloat16.Type = getType("iDB2DecFloat16");
            DB2iSeriesTypes.DecFloat34.Type = getType("iDB2DecFloat34");
            DB2iSeriesTypes.Decimal.Type = getType("iDB2Decimal");
            DB2iSeriesTypes.Double.Type = getType("iDB2Double");
            DB2iSeriesTypes.Graphic.Type = getType("iDB2Graphic");
            DB2iSeriesTypes.Integer.Type = getType("iDB2Integer");
            DB2iSeriesTypes.Numeric.Type = getType("iDB2Numeric");
            DB2iSeriesTypes.Real.Type = getType("iDB2Real");
            DB2iSeriesTypes.RowId.Type = getType("iDB2Rowid");
            DB2iSeriesTypes.SmallInt.Type = getType("iDB2SmallInt");
            DB2iSeriesTypes.Time.Type = getType("iDB2Time");
            DB2iSeriesTypes.TimeStamp.Type = getType("iDB2TimeStamp");
            DB2iSeriesTypes.VarBinary.Type = getType("iDB2VarBinary");
            DB2iSeriesTypes.VarChar.Type = getType("iDB2VarChar");
            DB2iSeriesTypes.VarCharBitData.Type = getType("iDB2VarCharBitData");
            DB2iSeriesTypes.VarGraphic.Type = getType("iDB2VarGraphic");
            DB2iSeriesTypes.Xml.Type = getType("iDB2Xml");

            SetProviderField(DB2iSeriesTypes.BigInt, typeof(long), "GetiDB2BigInt");
            SetProviderField(DB2iSeriesTypes.Binary, typeof(byte[]), "GetiDB2Binary");
            SetProviderField(DB2iSeriesTypes.Blob, typeof(byte[]), "GetiDB2Blob");
            SetProviderField(DB2iSeriesTypes.Char, typeof(string), "GetiDB2Char");
            SetProviderField(DB2iSeriesTypes.CharBitData, typeof(byte[]), "GetiDB2CharBitData");
            SetProviderField(DB2iSeriesTypes.Clob, typeof(string), "GetiDB2Clob");
            SetProviderField(DB2iSeriesTypes.DataLink, typeof(string), "GetiDB2DataLink");
            SetProviderField(DB2iSeriesTypes.Date, typeof(System.DateTime), "GetiDB2Date");
            SetProviderField(DB2iSeriesTypes.DbClob, typeof(string), "GetiDB2DbClob");
            SetProviderField(DB2iSeriesTypes.DecFloat16, typeof(decimal), "GetiDB2DecFloat16");
            SetProviderField(DB2iSeriesTypes.DecFloat34, typeof(decimal), "GetiDB2DecFloat34");
            SetProviderField(DB2iSeriesTypes.Decimal, typeof(decimal), "GetiDB2Decimal");
            SetProviderField(DB2iSeriesTypes.Double, typeof(double), "GetiDB2Double");
            SetProviderField(DB2iSeriesTypes.Graphic, typeof(string), "GetiDB2Graphic");
            SetProviderField(DB2iSeriesTypes.Integer, typeof(int), "GetiDB2Integer");
            SetProviderField(DB2iSeriesTypes.Numeric, typeof(decimal), "GetiDB2Numeric");
            SetProviderField(DB2iSeriesTypes.Real, typeof(float), "GetiDB2Real");
            SetProviderField(DB2iSeriesTypes.RowId, typeof(byte[]), "GetiDB2RowId");
            SetProviderField(DB2iSeriesTypes.SmallInt, typeof(short), "GetiDB2SmallInt");
            SetProviderField(DB2iSeriesTypes.Time, typeof(DateTime), "GetiDB2Time");
            SetProviderField(DB2iSeriesTypes.TimeStamp, typeof(DateTime), "GetiDB2TimeStamp");
            SetProviderField(DB2iSeriesTypes.VarBinary, typeof(byte[]), "GetiDB2VarBinary");
            SetProviderField(DB2iSeriesTypes.VarChar, typeof(string), "GetiDB2VarChar");
            SetProviderField(DB2iSeriesTypes.VarCharBitData, typeof(byte[]), "GetiDB2VarCharBitData");
            SetProviderField(DB2iSeriesTypes.VarGraphic, typeof(string), "GetiDB2VarGraphic");
            SetProviderField(DB2iSeriesTypes.Xml, typeof(string), "GetiDB2Xml");

            //MappingSchema.AddScalarType(DB2iSeriesTypes.BigInt, GetNullValue(DB2iSeriesTypes.BigInt), true, DataType.Int64);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Binary, GetNullValue(DB2iSeriesTypes.Binary), true, DataType.Binary);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Blob, GetNullValue(DB2iSeriesTypes.Blob), true, DataType.Blob);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Char, GetNullValue(DB2iSeriesTypes.Char), true, DataType.Char);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.CharBitData, GetNullValue(DB2iSeriesTypes.CharBitData), true, DataType.Binary);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Clob, GetNullValue(DB2iSeriesTypes.Clob), true, DataType.NText);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.DataLink, GetNullValue(DB2iSeriesTypes.DataLink), true, DataType.NText);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Date, GetNullValue(DB2iSeriesTypes.Date), true, DataType.Date);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.DbClob, GetNullValue(DB2iSeriesTypes.DbClob), true, DataType.NText);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.DecFloat16, GetNullValue(DB2iSeriesTypes.DecFloat16), true, DataType.Decimal);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.DecFloat34, GetNullValue(DB2iSeriesTypes.DecFloat34), true, DataType.Decimal);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Decimal, GetNullValue(DB2iSeriesTypes.Decimal), true, DataType.Decimal);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Double, GetNullValue(DB2iSeriesTypes.Double), true, DataType.Double);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Graphic, GetNullValue(DB2iSeriesTypes.Graphic), true, DataType.NText);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Integer, GetNullValue(DB2iSeriesTypes.Integer), true, DataType.Int32);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Numeric, GetNullValue(DB2iSeriesTypes.Numeric), true, DataType.Decimal);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Real, GetNullValue(DB2iSeriesTypes.Real), true, DataType.Single);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.RowId, GetNullValue(DB2iSeriesTypes.RowId), true, DataType.VarBinary);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.SmallInt, GetNullValue(DB2iSeriesTypes.SmallInt), true, DataType.Int16);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Time, GetNullValue(DB2iSeriesTypes.Time), true, DataType.Time);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.TimeStamp, GetNullValue(DB2iSeriesTypes.TimeStamp), true, DataType.DateTime2);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.VarBinary, GetNullValue(DB2iSeriesTypes.VarBinary), true, DataType.VarBinary);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.VarChar, GetNullValue(DB2iSeriesTypes.VarChar), true, DataType.VarChar);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.VarCharBitData, GetNullValue(DB2iSeriesTypes.VarCharBitData), true, DataType.VarBinary);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.VarGraphic, GetNullValue(DB2iSeriesTypes.VarGraphic), true, DataType.NText);
            //MappingSchema.AddScalarType(DB2iSeriesTypes.Xml, GetNullValue(DB2iSeriesTypes.Xml), true, DataType.Xml);

            _setBlob = GetSetParameter(connectionType, "iDB2Parameter", "iDB2DbType", "iDB2DbType", "iDB2Blob");
            
            
            //DB2iSeriesTools.Initialized();
        }

        private void OnConnectionTypeCreated_DB2Connect(Type connectionType)
        {
            foreach(var type in DB2Types.AllTypes)
                SetProviderField(type.Type, type.DotnetType, type.DatareaderGetMethodName);

            //SetProviderField(DB2Types.Instance.DB2Int64, typeof(Int64), "GetDB2Int64");
            //SetProviderField(DB2Types.Instance.DB2Int32, typeof(Int32), "GetDB2Int32");
            //SetProviderField(DB2Types.Instance.DB2Int16, typeof(Int16), "GetDB2Int16");
            //SetProviderField(DB2Types.Instance.DB2Decimal, typeof(Decimal), "GetDB2Decimal");
            //SetProviderField(DB2Types.Instance.DB2DecimalFloat, typeof(Decimal), "GetDB2DecimalFloat");
            //SetProviderField(DB2Types.Instance.DB2Real, typeof(Single), "GetDB2Real");
            //SetProviderField(DB2Types.Instance.DB2Real370, typeof(Single), "GetDB2Real370");
            //SetProviderField(DB2Types.Instance.DB2Double, typeof(Double), "GetDB2Double");
            //SetProviderField(DB2Types.Instance.DB2String, typeof(String), "GetDB2String");
            //SetProviderField(DB2Types.Instance.DB2Clob, typeof(String), "GetDB2Clob");
            //SetProviderField(DB2Types.Instance.DB2Binary, typeof(byte[]), "GetDB2Binary");
            //SetProviderField(DB2Types.Instance.DB2Blob, typeof(byte[]), "GetDB2Blob");
            //SetProviderField(DB2Types.Instance.DB2Date, typeof(DateTime), "GetDB2Date");
            //SetProviderField(DB2Types.Instance.DB2Time, typeof(TimeSpan), "GetDB2Time");
            //SetProviderField(DB2Types.Instance.DB2TimeStamp, typeof(DateTime), "GetDB2TimeStamp");
            //SetProviderField(DB2Types.Instance.DB2Xml, typeof(string), "GetDB2Xml");
            //SetProviderField(DB2Types.Instance.DB2RowId, typeof(byte[]), "GetDB2RowId");

            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Int64, GetNullValue(DB2.DB2Types.DB2Int64), true, DataType.Int64);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Int32, GetNullValue(DB2.DB2Types.DB2Int32), true, DataType.Int32);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Int16, GetNullValue(DB2.DB2Types.DB2Int16), true, DataType.Int16);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Decimal, GetNullValue(DB2.DB2Types.DB2Decimal), true, DataType.Decimal);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2DecimalFloat, GetNullValue(DB2.DB2Types.DB2DecimalFloat), true, DataType.Decimal);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Real, GetNullValue(DB2.DB2Types.DB2Real), true, DataType.Single);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Real370, GetNullValue(DB2.DB2Types.DB2Real370), true, DataType.Single);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Double, GetNullValue(DB2.DB2Types.DB2Double), true, DataType.Double);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2String, GetNullValue(DB2.DB2Types.DB2String), true, DataType.NVarChar);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Clob, GetNullValue(DB2.DB2Types.DB2Clob), true, DataType.NText);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Binary, GetNullValue(DB2.DB2Types.DB2Binary), true, DataType.VarBinary);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Blob, GetNullValue(DB2.DB2Types.DB2Blob), true, DataType.Blob);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Date, GetNullValue(DB2.DB2Types.DB2Date), true, DataType.Date);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Time, GetNullValue(DB2.DB2Types.DB2Time), true, DataType.Time);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2TimeStamp, GetNullValue(DB2.DB2Types.DB2TimeStamp), true, DataType.DateTime2);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2RowId, GetNullValue(DB2.DB2Types.DB2RowId), true, DataType.VarBinary);
            //MappingSchema.AddScalarType(DB2.DB2Types.DB2Xml, DB2.DB2Tools.IsCore ? null : GetNullValue(DB2.DB2Types.DB2Xml), true, DataType.Xml);

            _setBlob = GetSetParameter(connectionType, "DB2Parameter", "DB2Type", "DB2Type", "Blob");

            //Note: Removed the check for DateTime support , assumed not supported

            //if (DataConnection.TraceSwitch.TraceInfo)
            //{
            //    DataConnection.WriteTraceLine(
            //        DataReaderType.AssemblyEx().FullName,
            //        DataConnection.TraceSwitch.DisplayName);
            //}

            
        }

        protected override void OnConnectionTypeCreated(Type connectionType)
        {
            if (connectionType.Name == DB2iSeriesTools.GetConnectionTypeName(DB2iSeriesAdoProviderType.AccessClient))
                OnConnectionTypeCreated_AccessClient(connectionType);
            else if (connectionType.Name == DB2iSeriesTools.GetConnectionTypeName(DB2iSeriesAdoProviderType.DB2Connect))
                OnConnectionTypeCreated_DB2Connect(connectionType);
            else
                throw new NotSupportedException($"Unsupported connect type {connectionType.Name}");

            if (DataConnection.TraceSwitch.TraceInfo)
                DataConnection.WriteTraceLine(DataReaderType.AssemblyEx().FullName, DataConnection.TraceSwitch.DisplayName);

            DB2iSeriesTools.Initialized();
        }

        #endregion

        #region Overrides

        public override string ConnectionNamespace => DB2iSeriesTools.GetConnectionNamespace(options.AdoProviderType);
        protected override string ConnectionTypeName => DB2iSeriesTools.GetConnectionTypeName(options.AdoProviderType);
        protected override string DataReaderTypeName => DB2iSeriesTools.GetDataReaderTypeName(options.AdoProviderType);
        
        public override BulkCopyRowsCopied BulkCopy<T>(DataConnection dataConnection, BulkCopyOptions options, IEnumerable<T> source)
        {
            if (_bulkCopy == null)
                _bulkCopy = new DB2iSeriesBulkCopy();

            return _bulkCopy.BulkCopy(
              options.BulkCopyType == BulkCopyType.Default ? DB2iSeriesTools.DefaultBulkCopyType : options.BulkCopyType,
              dataConnection,
              options,
              source);
        }
        public override ISqlBuilder CreateSqlBuilder()
        {
            return options.MinLevel == DB2iSeriesLevels.V7_1_38 ?
                new DB2iSeriesSqlBuilder7_2(GetSqlOptimizer(), SqlProviderFlags, MappingSchema.ValueToSqlConverter) :
                new DB2iSeriesSqlBuilder(GetSqlOptimizer(), SqlProviderFlags, MappingSchema.ValueToSqlConverter);
        }

        public override ISchemaProvider GetSchemaProvider()
        {
            return new DB2iSeriesSchemaProvider();
        }
        public override ISqlOptimizer GetSqlOptimizer()
        {
            return _sqlOptimizer;
        }
        public override void InitCommand(DataConnection dataConnection, CommandType commandType, string commandText, DataParameter[] parameters)
        {
            dataConnection.DisposeCommand();

            base.InitCommand(dataConnection, commandType, commandText, parameters);
        }

        public override MappingSchema MappingSchema
        {
            get
            {
                if (options.AdoProviderType == DB2iSeriesAdoProviderType.AccessClient)
                {
                    return options.MapGuidAsString
                      ? DB2iSeriesAccessClientMappingSchema_GAS.Instance as MappingSchema
                      : DB2iSeriesAccessClientMappingSchema.Instance;
                }
                else if (options.AdoProviderType == DB2iSeriesAdoProviderType.DB2Connect)
                {
                    return options.MapGuidAsString
                      ? DB2iSeriesDB2ConnectMappingSchema_GAS.Instance as MappingSchema
                      : DB2iSeriesDB2ConnectMappingSchema.Instance;
                }
                else
                    throw new NotSupportedException();
            }
        }

        protected override IDbConnection CreateConnectionInternal(string connectionString)
        {
            return DB2iSeriesTools.CreateConnection(options.AdoProviderType, connectionString);
        }

        #region Merge
        public override int Merge<T>(DataConnection dataConnection, Expression<Func<T, bool>> deletePredicate, bool delete, IEnumerable<T> source, string tableName, string databaseName, string schemaName)
        {
            if (delete)
            {
                throw new LinqToDBException("DB2 iSeries MERGE statement does not support DELETE by source.");
            }
            return new DB2iSeriesMerge().Merge(dataConnection, deletePredicate, delete, source, tableName, databaseName, schemaName);
        }

	    public override Task<int> MergeAsync<T>(DataConnection dataConnection, Expression<Func<T, bool>> deletePredicate, bool delete, IEnumerable<T> source,
		    string tableName, string databaseName, string schemaName, CancellationToken token)
	    {
		    if (delete)
			    throw new LinqToDBException("DB2 MERGE statement does not support DELETE by source.");

		    return new DB2iSeriesMerge().MergeAsync(dataConnection, deletePredicate, delete, source, tableName, databaseName, schemaName, token);
	    }

	    protected override BasicMergeBuilder<TTarget, TSource> GetMergeBuilder<TTarget, TSource>(
		    DataConnection connection,
		    IMergeable<TTarget, TSource> merge)
	    {
		    return new DB2iSeriesMergeBuilder<TTarget, TSource>(connection, merge);
	    }

        #endregion

        

        public override void SetParameter(IDbDataParameter parameter, string name, DataType dataType, object value)
        {
            if (value is sbyte)
            {
                value = Convert.ToInt16(Convert.ToSByte(value));
                dataType = DataType.Int16;
            }
            else if (value is byte)
            {
                value = Convert.ToInt16(Convert.ToByte(value));
                dataType = DataType.Int16;
            }

            switch (dataType)
            {
                case DataType.UInt16:
                    dataType = DataType.Int32;
                    if (value != null)
                        value = Convert.ToInt32(value);
                    break;
                case DataType.UInt32:
                    dataType = DataType.Int64;
                    if (value != null)
                        value = Convert.ToInt64(value);
                    break;
                case DataType.UInt64:
                    dataType = DataType.Decimal;
                    if (value != null)
                        value = Convert.ToDecimal(value);
                    break;
                case DataType.VarNumeric:
                    dataType = DataType.Decimal;
                    break;
                case DataType.Char:
                case DataType.VarChar:
                case DataType.NChar:
                case DataType.NVarChar:
                    if (value is Guid)
                    {
                        value = ((Guid)value).ToString("D");
                    }
                    else if (value is bool)
                    {
                        value = Common.ConvertTo<char>.From(value);
                    }
                    break;
                case DataType.Boolean:
                case DataType.Int16:
                    if (value is bool)
                    {
                        value = (bool)value ? 1 : 0;
                        dataType = DataType.Int16;
                    }
                    break;
                case DataType.Guid:
                    if (value is Guid)
                    {
                        if (options.MapGuidAsString)
                        {
                            value = ((Guid)value).ToString("D");
                            dataType = DataType.NVarChar;
                        }
                        else
                        {
                            value = ((Guid)value).ToByteArray();
                            dataType = DataType.VarBinary;
                        }
                    }
                    if (value == null)
                        dataType = DataType.VarBinary;
                    break;
                case DataType.Binary:
                case DataType.VarBinary:
                    if (value is Guid) value = ((Guid)value).ToByteArray();
                    else if (parameter.Size == 0 && value != null && value.GetType().Name == "DB2Binary")
                    {
                        dynamic v = value;
                        if (v.IsNull)
                            value = DBNull.Value;
                    }

                    break;
                case DataType.Time:
                    if (value is TimeSpan)
                    {
                        value = new DateTime(((TimeSpan)value).Ticks);
                    }
                    break;
                case DataType.DateTime2:
                    dataType = DataType.DateTime;
                    break;
                case DataType.Blob:
                    base.SetParameter(parameter, $"@{name}", dataType, value);
                    _setBlob(parameter);
                    return;
            }
            base.SetParameter(parameter, $"@{name}", dataType, value);
        }

        #endregion

        #region Helpers

		private static void LoadExpressions(string providerName, bool mapGuidAsString)
		{
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Space(0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Int32?, String>(p0 => Sql.Convert(Sql.VarChar(1000), Linq.Expressions.Replicate(" ", p0)))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Stuff("", 0, 0, "")),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Int32?, String, String>((p0, p1, p2, p3) => Linq.Expressions.AltStuff(p0, p1, p2, p3))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.PadRight("", 0, ' ')),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Char?, String>((p0, p1, p2) => p0.Length > p1 ? p0 : p0 + Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.PadLeft("", 0, ' ')),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Char?, String>((p0, p1, p2) => p0.Length > p1 ? p0 : Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000) + p0)));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.ConvertTo<String>.From((Decimal)0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Decimal, String>((Decimal p) => Sql.TrimLeft(Sql.Convert<string, Decimal>(p), '0'))));

			if (!mapGuidAsString)
			{
				Linq.Expressions.MapMember(
					providerName,
					Linq.Expressions.M(() => Sql.ConvertTo<String>.From(Guid.Empty)),
					Linq.Expressions.N(() => Linq.Expressions.L<Guid, String>(
						(Guid p) => Sql.Lower(Sql.Substring(Linq.Expressions.Hex(p), 7, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 5, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 3, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 1, 2)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 11, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 9, 2)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 15, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 13, 2)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 17, 4)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 21, 12)))));
			}

			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Log(0m, 0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Decimal?, Decimal?, Decimal?>((m, n) => Sql.Log(n) / Sql.Log(m))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Log(0.0, 0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Double?, Double?, Double?>((m, n) => Sql.Log(n) / Sql.Log(m))));
		}
	}

    #endregion
}