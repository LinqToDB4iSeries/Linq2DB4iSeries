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

        private static Action<IDbDataParameter> _setBlob_AccessClient;
        private static Action<IDbDataParameter> _setBlob_DB2Connect;
        private static Action<IDbDataParameter> GetBlobSetter(DB2iSeriesDataProvider provider)
        {

            if (provider.options.AdoProviderType == DB2iSeriesAdoProviderType.AccessClient)
            {
                if (_setBlob_AccessClient == null)
                    _setBlob_AccessClient = provider.GetSetParameter(DB2iSeriesTypes.ConnectionType, "iDB2Parameter", "iDB2DbType", "iDB2DbType", "iDB2Blob");

                return _setBlob_AccessClient;
            }
            else
            {
                if (_setBlob_DB2Connect == null)
                    _setBlob_DB2Connect = provider.GetSetParameter(DB2Types.ConnectionType,  "DB2Parameter", "DB2Type", "DB2Type", "Blob");

                return _setBlob_DB2Connect;
            }
        }


        private readonly DB2iSeriesDataProviderOptions options;
        private readonly DB2iSeriesSqlOptimizer _sqlOptimizer;
        private DB2iSeriesBulkCopy _bulkCopy;

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
            SqlProviderFlags.CanCombineParameters = options.AdoProviderType == DB2iSeriesAdoProviderType.DB2Connect;
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

        private void OnConnectionTypeCreated_AccessClient()
        {
            foreach (var type in DB2iSeriesTypes.AllTypes)
                SetProviderField(type.Type, type.DotnetType, type.DatareaderGetMethodName);
        }

        private void OnConnectionTypeCreated_DB2Connect()
        {
            foreach(var type in DB2Types.AllTypes)
                SetProviderField(type.Type, type.DotnetType, type.DatareaderGetMethodName);
        }

        protected override void OnConnectionTypeCreated(Type connectionType)
        {
            if (connectionType.Name == DB2iSeriesTools.GetConnectionTypeName(DB2iSeriesAdoProviderType.AccessClient))
                OnConnectionTypeCreated_AccessClient();
            else if (connectionType.Name == DB2iSeriesTools.GetConnectionTypeName(DB2iSeriesAdoProviderType.DB2Connect))
                OnConnectionTypeCreated_DB2Connect();
            else
                throw new NotSupportedException($"Unsupported connect type {connectionType.Name}");

            if (DataConnection.TraceSwitch.TraceInfo)
                DataConnection.WriteTraceLine(DataReaderType.AssemblyEx().FullName, DataConnection.TraceSwitch.DisplayName);

            //Obsolete - left in as not to cause breaking changes
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
                new DB2iSeriesSqlBuilder7_2(GetSqlOptimizer(), SqlProviderFlags, MappingSchema.ValueToSqlConverter, options) :
                new DB2iSeriesSqlBuilder(GetSqlOptimizer(), SqlProviderFlags, MappingSchema.ValueToSqlConverter, options);
        }

        public override ISchemaProvider GetSchemaProvider()
        {
            return new DB2iSeriesSchemaProvider(options.AdoProviderType);
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
            if (options.AdoProviderType == DB2iSeriesAdoProviderType.AccessClient)
            {
                var csb = DB2iSeriesTools.CreateConnectionStringBuilder(DB2iSeriesAdoProviderType.AccessClient, connectionString);
                csb["Naming"] = ((int)DB2iSeriesNamingConvention.Sql).ToString();
                connectionString = csb.ToString();
            }
            
            return DB2iSeriesTools.CreateConnection(options.AdoProviderType, connectionString);
        }

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
                    if (parameter.GetType().Assembly.GetName().Name == DB2iSeriesTypes.AssemblyName)
                    {
                        if (value is TimeSpan)
                        {
                            value = new DateTime(((TimeSpan)value).Ticks);
                        }
                    }
                    break;
                case DataType.DateTime2:
                    dataType = DataType.DateTime;
                    break;
                case DataType.Blob:
                    base.SetParameter(parameter, $"@{name}", dataType, value);
                    GetBlobSetter(this)(parameter);
                    return;
            }
            base.SetParameter(parameter, $"@{name}", dataType, value);
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

        #endregion
    }
}