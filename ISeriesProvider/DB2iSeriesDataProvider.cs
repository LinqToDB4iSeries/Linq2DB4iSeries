using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using LinqToDB.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using Data;
    using Extensions;
    using Mapping;
    using SchemaProvider;
    using SqlProvider;

    public class DB2iSeriesDataProvider : DynamicDataProviderBase
    {
	    private DB2iSeriesLevels minLevel;

	    private bool mapGuidAsString;

	    public DB2iSeriesDataProvider() : this(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false)
	    {
	    }

	    public DB2iSeriesDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString) : base(name, null)
        {
            this.minLevel = minLevel;
	        this.mapGuidAsString = mapGuidAsString;

            LoadExpressions(name, mapGuidAsString);

            SqlProviderFlags.AcceptsTakeAsParameter = false;
            SqlProviderFlags.AcceptsTakeAsParameterIfSkip = true;
            SqlProviderFlags.IsDistinctOrderBySupported = false;
            SqlProviderFlags.CanCombineParameters = false;
            SqlProviderFlags.IsParameterOrderDependent = true;
            SqlProviderFlags.IsCommonTableExpressionsSupported = true;
            
			if(mapGuidAsString)
				SqlProviderFlags.CustomFlags.Add(DB2iSeriesTools.MapGuidAsString);

            SetCharField("CHAR", (r, i) => r.GetString(i).TrimEnd(' '));
            SetCharField("NCHAR", (r, i) => r.GetString(i).TrimEnd(' '));

            _sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags);
        }

        
        readonly DB2iSeriesSqlOptimizer _sqlOptimizer;
        static Action<IDbDataParameter> _setBlob;
        DB2iSeriesBulkCopy _bulkCopy;

        #region "overrides"

        public override string ConnectionNamespace => "";
        protected override string ConnectionTypeName => DB2iSeriesTools.ConnectionTypeName;
        protected override string DataReaderTypeName => DB2iSeriesTools.DataReaderTypeName;
        public override string DbFactoryProviderName => "IBM.Data.DB2.iSeries";

        public string DummyTableName => DB2iSeriesTools.iSeriesDummyTableName();

        public override BulkCopyRowsCopied BulkCopy<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source)
        {
            if (_bulkCopy == null)
                _bulkCopy = new DB2iSeriesBulkCopy();

            return _bulkCopy.BulkCopy(
              options.BulkCopyType == BulkCopyType.Default ? DB2iSeriesTools.DefaultBulkCopyType : options.BulkCopyType,
              table,
              options,
              source);
        }
        public override ISqlBuilder CreateSqlBuilder(MappingSchema mappingSchema)
        {
            return minLevel == DB2iSeriesLevels.V7_1_38 ?
                new DB2iSeriesSqlBuilder7_2(GetSqlOptimizer(), SqlProviderFlags, mappingSchema.ValueToSqlConverter) :
                new DB2iSeriesSqlBuilder(GetSqlOptimizer(), SqlProviderFlags, mappingSchema.ValueToSqlConverter);
        }

        public override ISchemaProvider GetSchemaProvider()
        {
            return new DB2iSeriesSchemaProvider();
        }
        public override ISqlOptimizer GetSqlOptimizer()
        {
            return _sqlOptimizer;
        }
        public override void InitCommand(DataConnection dataConnection, CommandType commandType, string commandText, DataParameter[] parameters, bool withParameters)
        {
            dataConnection.DisposeCommand();

            base.InitCommand(dataConnection, commandType, commandText, parameters, withParameters);
        }

	  //  protected override IDbConnection CreateConnectionInternal(string connectionString)
	  //  {
	  //      var vers73 = new[] { "7.1.38", "7.2", "7.3" };
		 //   var parts = connectionString.Split(';').ToList();
		 //   var gas = parts.FirstOrDefault(p => p.ToLower().StartsWith("mapguidasstring"));
			//var minVer = parts.FirstOrDefault(p => p.ToLower().StartsWith("minver"));

		 //   if (!string.IsNullOrWhiteSpace(gas))
		 //   {
			//    mapGuidAsString = gas.EndsWith("true", StringComparison.CurrentCultureIgnoreCase);
			//    parts.Remove(gas);
		 //   }

		 //   if (!string.IsNullOrWhiteSpace(minVer))
		 //   {
			//	minLevel = vers73.Any(v => minVer.EndsWith(v, StringComparison.CurrentCultureIgnoreCase)) ? DB2iSeriesLevels.V7_1_38 : DB2iSeriesLevels.Any;
			//	parts.Remove(minVer);
			//}

		 //   var conString = string.Join(";", parts);
			//return base.CreateConnectionInternal(conString);
	  //  }

	    static class MappingSchemaInstance
	    {
		    public static readonly DB2iSeriesMappingSchema BlobGuidMappingSchema = new DB2iSeriesMappingSchema(DB2iSeriesProviderName.DB2); 
		    public static readonly DB2iSeriesMappingSchema StringGuidMappingSchema = new DB2iSeriesMappingSchema(DB2iSeriesProviderName.DB2_GAS);
	    }

		public override MappingSchema MappingSchema => mapGuidAsString
		    ? MappingSchemaInstance.StringGuidMappingSchema
		    : MappingSchemaInstance.BlobGuidMappingSchema;

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

		protected override void OnConnectionTypeCreated(Type connectionType)
        {
            DB2iSeriesTypes.ConnectionType = connectionType;

            dynamic ass = connectionType.Assembly;
            DB2iSeriesTypes.BigInt.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2BigInt", true);
            DB2iSeriesTypes.Binary.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Binary", true);
            DB2iSeriesTypes.Blob.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Blob", true);
            DB2iSeriesTypes.Char.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Char", true);
            DB2iSeriesTypes.CharBitData.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2CharBitData", true);
            DB2iSeriesTypes.Clob.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Clob", true);
            DB2iSeriesTypes.Date.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Date", true);
            DB2iSeriesTypes.DataLink.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2DataLink", true);
            DB2iSeriesTypes.DbClob.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2DbClob", true);
            DB2iSeriesTypes.DecFloat16.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2DecFloat16", true);
            DB2iSeriesTypes.DecFloat34.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2DecFloat34", true);
            DB2iSeriesTypes.Decimal.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Decimal", true);
            DB2iSeriesTypes.Double.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Double", true);
            DB2iSeriesTypes.Graphic.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Graphic", true);
            DB2iSeriesTypes.Integer.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Integer", true);
            DB2iSeriesTypes.Numeric.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Numeric", true);
            DB2iSeriesTypes.Real.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Real", true);
            DB2iSeriesTypes.RowId.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Rowid", true);
            DB2iSeriesTypes.SmallInt.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2SmallInt", true);
            DB2iSeriesTypes.Time.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Time", true);
            DB2iSeriesTypes.TimeStamp.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2TimeStamp", true);
            DB2iSeriesTypes.VarBinary.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2VarBinary", true);
            DB2iSeriesTypes.VarChar.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2VarChar", true);
            DB2iSeriesTypes.VarCharBitData.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2VarCharBitData", true);
            DB2iSeriesTypes.VarGraphic.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2VarGraphic", true);
            DB2iSeriesTypes.Xml.Type = ass.GetType(DB2iSeriesTools.AssemblyName + ".iDB2Xml", true);

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
            SetProviderField(DB2iSeriesTypes.Time, typeof(System.DateTime), "GetiDB2Time");
            SetProviderField(DB2iSeriesTypes.TimeStamp, typeof(System.DateTime), "GetiDB2TimeStamp");
            SetProviderField(DB2iSeriesTypes.VarBinary, typeof(byte[]), "GetiDB2VarBinary");
            SetProviderField(DB2iSeriesTypes.VarChar, typeof(string), "GetiDB2VarChar");
            SetProviderField(DB2iSeriesTypes.VarCharBitData, typeof(byte[]), "GetiDB2VarCharBitData");
            SetProviderField(DB2iSeriesTypes.VarGraphic, typeof(string), "GetiDB2VarGraphic");
            SetProviderField(DB2iSeriesTypes.Xml, typeof(string), "GetiDB2Xml");

            MappingSchema.AddScalarType(DB2iSeriesTypes.BigInt, GetNullValue(DB2iSeriesTypes.BigInt), true, DataType.Int64);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Binary, GetNullValue(DB2iSeriesTypes.Binary), true, DataType.Binary);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Blob, GetNullValue(DB2iSeriesTypes.Blob), true, DataType.Blob);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Char, GetNullValue(DB2iSeriesTypes.Char), true, DataType.Char);
            MappingSchema.AddScalarType(DB2iSeriesTypes.CharBitData, GetNullValue(DB2iSeriesTypes.CharBitData), true, DataType.Binary);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Clob, GetNullValue(DB2iSeriesTypes.Clob), true, DataType.NText);
            MappingSchema.AddScalarType(DB2iSeriesTypes.DataLink, GetNullValue(DB2iSeriesTypes.DataLink), true, DataType.NText);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Date, GetNullValue(DB2iSeriesTypes.Date), true, DataType.Date);
            MappingSchema.AddScalarType(DB2iSeriesTypes.DbClob, GetNullValue(DB2iSeriesTypes.DbClob), true, DataType.NText);
            MappingSchema.AddScalarType(DB2iSeriesTypes.DecFloat16, GetNullValue(DB2iSeriesTypes.DecFloat16), true, DataType.Decimal);
            MappingSchema.AddScalarType(DB2iSeriesTypes.DecFloat34, GetNullValue(DB2iSeriesTypes.DecFloat34), true, DataType.Decimal);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Decimal, GetNullValue(DB2iSeriesTypes.Decimal), true, DataType.Decimal);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Double, GetNullValue(DB2iSeriesTypes.Double), true, DataType.Double);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Graphic, GetNullValue(DB2iSeriesTypes.Graphic), true, DataType.NText);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Integer, GetNullValue(DB2iSeriesTypes.Integer), true, DataType.Int32);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Numeric, GetNullValue(DB2iSeriesTypes.Numeric), true, DataType.Decimal);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Real, GetNullValue(DB2iSeriesTypes.Real), true, DataType.Single);
            MappingSchema.AddScalarType(DB2iSeriesTypes.RowId, GetNullValue(DB2iSeriesTypes.RowId), true, DataType.VarBinary);
            MappingSchema.AddScalarType(DB2iSeriesTypes.SmallInt, GetNullValue(DB2iSeriesTypes.SmallInt), true, DataType.Int16);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Time, GetNullValue(DB2iSeriesTypes.Time), true, DataType.Time);
            MappingSchema.AddScalarType(DB2iSeriesTypes.TimeStamp, GetNullValue(DB2iSeriesTypes.TimeStamp), true, DataType.DateTime2);
            MappingSchema.AddScalarType(DB2iSeriesTypes.VarBinary, GetNullValue(DB2iSeriesTypes.VarBinary), true, DataType.VarBinary);
            MappingSchema.AddScalarType(DB2iSeriesTypes.VarChar, GetNullValue(DB2iSeriesTypes.VarChar), true, DataType.VarChar);
            MappingSchema.AddScalarType(DB2iSeriesTypes.VarCharBitData, GetNullValue(DB2iSeriesTypes.VarCharBitData), true, DataType.VarBinary);
            MappingSchema.AddScalarType(DB2iSeriesTypes.VarGraphic, GetNullValue(DB2iSeriesTypes.VarGraphic), true, DataType.NText);
            MappingSchema.AddScalarType(DB2iSeriesTypes.Xml, GetNullValue(DB2iSeriesTypes.Xml), true, DataType.Xml);
            _setBlob = GetSetParameter(connectionType, "iDB2Parameter", "iDB2DbType", "iDB2DbType", "iDB2Blob");
            if (DataConnection.TraceSwitch.TraceInfo)
            {
                DataConnection.WriteTraceLine(DataReaderType.AssemblyEx().FullName, DataConnection.TraceSwitch.DisplayName);
            }
            DB2iSeriesTools.Initialized();
        }

        public override void SetParameter(IDbDataParameter parameter, string name, DbDataType dataType, object value)
        {
            if (value is sbyte)
            {
                value = (short)(sbyte)value;
                dataType = dataType.WithDataType(DataType.Int16);
            }
            else if (value is byte)
            {
                value = (short)(byte)value;
                dataType = dataType.WithDataType(DataType.Int16);
            }

            switch (dataType.DataType)
            {
                case DataType.UInt16:
                    dataType = dataType.WithDataType(DataType.Int32);
                    if (value != null)
                        value = Convert.ToInt32(value);
                    break;
                case DataType.UInt32:
                    dataType = dataType.WithDataType(DataType.Int64);
                    if (value != null)
                        value = Convert.ToInt64(value);
                    break;
                case DataType.UInt64:
                    dataType = dataType.WithDataType(DataType.Decimal);
                    if (value != null)
                        value = Convert.ToDecimal(value);
                    break;
                case DataType.VarNumeric: dataType = dataType.WithDataType(DataType.Decimal); break;
                case DataType.DateTime2: dataType = dataType.WithDataType(DataType.DateTime); break;
                case DataType.Char:
                case DataType.VarChar:
                case DataType.NChar:
                case DataType.NVarChar:
                    if (value is Guid) value = ((Guid)value).ToString("D");
                    else if (value is bool)
                        value = Common.ConvertTo<char>.From((bool)value);
                    break;
                case DataType.Boolean:
                case DataType.Int16:
                    if (value is bool)
                    {
                        value = (bool)value ? 1 : 0;
                        dataType = dataType.WithDataType(DataType.Int16);
                    }
                    break;
                case DataType.Guid:
                    if (value is Guid)
                    {
                        if (mapGuidAsString)
                        {
                            value = ((Guid)value).ToString("D");
                            dataType = dataType.WithDataType(DataType.NVarChar);
                        }
                        else
                        {
                            value = ((Guid) value).ToByteArray();
                            dataType = dataType.WithDataType(DataType.VarBinary);
                        }
                    }
                    if (value == null)
                        dataType = dataType.WithDataType(DataType.VarBinary);
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
                case DataType.Blob:
                    base.SetParameter(parameter, "@" + name, dataType, value);
                    _setBlob(parameter);
                    return;
            }

            base.SetParameter(parameter, "@" + name, dataType, value);
        }
        #endregion

        private static object GetNullValue(Type type)
        {
            dynamic getValue = Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, type, "Null"), typeof(object)));
            return getValue.Compile()();
        }

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
}