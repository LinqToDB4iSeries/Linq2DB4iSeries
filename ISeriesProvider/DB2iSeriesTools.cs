using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using LinqToDB.Common;
using LinqToDB.Extensions;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using System.Data.Common;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Configuration;
    using Data;

    public static class DB2iSeriesTools
    {
        #region Reflection Constants

        internal const string AssemblyName_AccessClient = "IBM.Data.DB2.iSeries";
        internal const string AssemblyName_DB2Connect_Net = "IBM.Data.DB2";
        internal const string AssemblyName_DB2Connect_Core = "IBM.Data.DB2.Core";
        internal const string NamespaceNameDB2Types = "IBM.Data.DB2Types";
        

        internal static readonly string AssemblyName_DB2Connect = DB2.DB2Tools.AssemblyName;
        internal static readonly string NamespaceName_DB2Connect = AssemblyName_DB2Connect;

        internal static readonly string ConnectionTypeName_AccessClient = AssemblyName_AccessClient + ".iDB2Connection, " + AssemblyName_AccessClient;
        internal static readonly string DataReaderTypeName_AccessClient = AssemblyName_AccessClient + ".iDB2DataReader, " + AssemblyName_AccessClient;

        internal static readonly string ConnectionTypeName_DB2Connect = AssemblyName_DB2Connect + ".DB2Connection, " + AssemblyName_DB2Connect;
        internal static readonly string DataReaderTypeName_DB2Connect = AssemblyName_DB2Connect + ".DB2DataReader, " + AssemblyName_DB2Connect;

        internal static readonly string TypeNameDB2ConnectionStringBuilder_AccessClient = AssemblyName_AccessClient + ".iDB2ConnectionStringBuilder";
        internal static readonly string TypeNameDB2ConnectionStringBuilder_DB2Connect = AssemblyName_DB2Connect + ".DB2ConnectionStringBuilder";

        internal static readonly Lazy<Type> ConnectionType_AccessClient = new Lazy<Type>(() => Type.GetType(GetConnectionTypeName(DB2iSeriesAdoProviderType.AccessClient), true));
        internal static readonly Lazy<Type> ConnectionType_DB2Connect = new Lazy<Type>(() => Type.GetType(GetConnectionTypeName(DB2iSeriesAdoProviderType.DB2Connect), true));

        public static string GetConnectionTypeName(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            switch (dB2AdoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return ConnectionTypeName_DB2Connect;
                case DB2iSeriesAdoProviderType.AccessClient:
                    return ConnectionTypeName_AccessClient;
            }

            throw new NotSupportedException();
        }

        public static string GetDataReaderTypeName(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            switch (dB2AdoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DataReaderTypeName_DB2Connect;
                case DB2iSeriesAdoProviderType.AccessClient:
                    return DataReaderTypeName_AccessClient;
            }

            throw new NotSupportedException();
        }

        public static string GetConnectionNamespace(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            switch (dB2AdoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return NamespaceName_DB2Connect;
                case DB2iSeriesAdoProviderType.AccessClient:
                    return AssemblyName_AccessClient;
            }

            throw new NotSupportedException();
        }

        public static Type GetConnectionType(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            if (dB2AdoProviderType == DB2iSeriesAdoProviderType.AccessClient)
                return ConnectionType_AccessClient.Value;
            else if (dB2AdoProviderType == DB2iSeriesAdoProviderType.DB2Connect)
                return ConnectionType_DB2Connect.Value;
            else
                throw new NotSupportedException();
        }

        public static DB2iSeriesAdoProviderType? GetAdoProviderType(IDbConnection connection)
        {
            if (connection == null)
                return null;

            var connectionType = connection.GetType();
            if (connectionType == GetConnectionType(DB2iSeriesAdoProviderType.AccessClient))
                return DB2iSeriesAdoProviderType.AccessClient;
            else if (connectionType == GetConnectionType(DB2iSeriesAdoProviderType.DB2Connect))
                return DB2iSeriesAdoProviderType.DB2Connect;
            else
                return null;
        }

        #endregion

        #region Public Settings

        public static bool AutoDetectProvider { get; set; }
        public static DB2iSeriesAdoProviderType DefaultAdoProviderType { get; set; } = DB2iSeriesAdoProviderType.AccessClient;

        #endregion

        #region Static Constructor

        static DB2iSeriesTools()
        {
            AutoDetectProvider = true;

            //Default providers - for compatibility with the original provider names
            //Caution: this is unaffected by DefaultAdoProviderType - possibly set DefaultAdoProviderType as a const for compile type configuration of default only
            DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2, GetDataProvider(DB2iSeriesProviderName.DB2));
            DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2_GAS, GetDataProvider(DB2iSeriesProviderName.DB2_GAS));
            DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2_73, GetDataProvider(DB2iSeriesProviderName.DB2_73));
            DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2_73_GAS, GetDataProvider(DB2iSeriesProviderName.DB2_73_GAS));

            foreach (var item in dataProviders)
                DataConnection.AddDataProvider(item.Key, item.Value);

            DataConnection.AddProviderDetector(ProviderDetector);
        }

        #endregion

        #region SQL Tools

        internal const string IdentityColumnSql = "identity_val_local()";

        //Changed default naming convention to SQL as DB2 doesn't support it
        public static string iSeriesDummyTableName(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.Sql)
        {
            var seperator = (naming == DB2iSeriesNamingConvention.System) ? "/" : ".";
            return string.Format("SYSIBM{0}SYSDUMMY1", seperator);
        }

        #endregion

        #region DataProvider Cache

        //All instances of unique data providers
        static readonly IReadOnlyDictionary<string, DB2iSeriesDataProvider> actualDataProviders = 
            DB2iSeriesProviderName.AllNames
                .Where(x => !DB2iSeriesProviderName.IsVirtual(x))
                .Select(x => new DB2iSeriesDataProvider(x))
            .ToDictionary(x => x.Name);

        //All data providers dictionary, including virtual/mapped ones
        static readonly IReadOnlyDictionary<string, DB2iSeriesDataProvider> dataProviders =
            DB2iSeriesProviderName.AllNames
                .ToDictionary(x => x, x => actualDataProviders[DB2iSeriesProviderName.GetActualProviderName(x)]);

        public static DB2iSeriesDataProvider GetDataProvider(string providerName)
        {
            if (TryGetDataProvider(providerName, out var dataProvider))
                return dataProvider;

            throw new LinqToDBException($"There is no provider named {providerName} in the Linq2db iSeries provider name registry.");
        }

        public static bool TryGetDataProvider(string providerName, out DB2iSeriesDataProvider dataProvider)
        {
            return dataProviders.TryGetValue(providerName, out dataProvider);
        }

        #endregion

        #region Provider Detector

        private static DB2iSeriesAdoProviderType DetectFromConnectionString(string connectionString)
        {
            var tmpcs =
                connectionString
                    .Replace(" ", "") //Remove spaces
                    .ToLower();

            //AccessClient uses "DataSource" while DB2Connect uses "Server"
            return tmpcs.StartsWith("datasource=") || tmpcs.Contains(";datasource=") ?
                    DB2iSeriesAdoProviderType.AccessClient :
                    DB2iSeriesAdoProviderType.DB2Connect;
        }

        private static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
        {
            if (css.IsGlobal)
                return null;

            if (TryGetDataProvider(css.Name, out var dataProvider))
                return dataProvider;

            if (css.ProviderName == AssemblyName_AccessClient)
                return GetDataProvider(DB2iSeriesProviderName.DB2iSeries_AccessClient);
            else if (css.ProviderName == AssemblyName_DB2Connect || css.ProviderName == AssemblyName_DB2Connect_Core)
                return GetDataProvider(DB2iSeriesProviderName.DB2iSeries_DB2Connect);

            if (AutoDetectProvider)
            {
                try
                {
                    var cs = string.IsNullOrWhiteSpace(connectionString) ? css.ConnectionString : connectionString;
                    var providerType = DetectFromConnectionString(connectionString);

                    if (providerType == DB2iSeriesAdoProviderType.AccessClient)
                    {
                        using (var conn = ConnectionBuilder_ClientAccess(cs))
                        {
                            conn.Open();

                            if (MemberAccessor.TryGetValue<string>(conn, "ServerVersion", out var serverVersion))
                            {
                                serverVersion = serverVersion.Substring(0, 5);

                                string ptf;
                                int desiredLevel;

                                switch (serverVersion)
                                {
                                    case "07.03":
                                        return GetDataProvider(DB2iSeriesProviderName.DB2iSeries_AccessClient_73);
                                    case "07.02":
                                        ptf = "SF99702";
                                        desiredLevel = 9;
                                        break;
                                    case "07.01":
                                        ptf = "SF99701";
                                        desiredLevel = 38;
                                        break;
                                    default:
                                        return GetDataProvider(DB2iSeriesProviderName.DB2iSeries_AccessClient);
                                }
                                var level = GetLevel(conn, ptf);
                                return GetDataProvider(level < desiredLevel ? DB2iSeriesProviderName.DB2iSeries_AccessClient : DB2iSeriesProviderName.DB2iSeries_AccessClient_73);
                            }
                        }
                    }
                    else
                    {
                        //TODO: Detect DB2Connect provider
                        //ReflectionHelper.TryGetValue<string>(conn, "ServerVersion", out var eServerType))
                        //using (var conn = ConnectionBuilder_DB2Connect(cs))
                        //{
                        //    conn.Open();

                        //}
                        return GetDataProvider(DB2iSeriesProviderName.DB2iSeries_DB2Connect);
                    }
                }
                catch (Exception)
                {
                    return null;
                }
            }

            return null;
        }

        private static int GetLevel(IDbConnection conn, string ptf)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT MAX(PTF_GROUP_LEVEL) FROM QSYS2.GROUP_PTF_INFO WHERE PTF_GROUP_NAME = @p1 AND PTF_GROUP_STATUS = 'INSTALLED'";
                var param = cmd.CreateParameter();
                param.ParameterName = "p1";
                param.Value = ptf;

                cmd.Parameters.Add(param);

                return Converter.ChangeTypeTo<int>(cmd.ExecuteScalar());
            }
        }

        #endregion

        #region OnInitialized

        private static bool _isInitialized;
        private static readonly object _syncAfterInitialized = new object();
        private static ConcurrentBag<Action> _afterInitializedActions = new ConcurrentBag<Action>();

        internal static void Initialized()
        {
            if (!_isInitialized)
            {
                lock (_syncAfterInitialized)
                {
                    if (!_isInitialized)
                    {
                        _isInitialized = true;
                        foreach (var action in _afterInitializedActions)
                        {
                            action();
                        }
                        _afterInitializedActions = null;
                    }
                }
            }
        }

        public static void AfterInitialized(Action action)
        {
            if (_isInitialized)
            {
                action();
            }
            else
            {
                lock (_syncAfterInitialized)
                {
                    if (_isInitialized)
                    {
                        action();
                    }
                    else
                    {
                        _afterInitializedActions.Add(action);
                    }
                }
            }
        }

        #endregion

        #region CreateConnectionStringBuilder

        public static DB2iSeriesNamingConvention GetNamingConvention(string connectionString)
        {
            var providerType = DetectFromConnectionString(connectionString);
            if (providerType == DB2iSeriesAdoProviderType.DB2Connect)
                return DB2iSeriesNamingConvention.Sql;
            else
            {
                var csb = CreateConnectionStringBuilder(providerType, connectionString);
                return csb["Naming"]?.ToString() == "1" ? DB2iSeriesNamingConvention.System : DB2iSeriesNamingConvention.Sql;
            }

        }

        public static string GetSqlObjectDelimiter(string connectionString)
        {
            return GetNamingConvention(connectionString) == DB2iSeriesNamingConvention.Sql ? "." : "/";
        }

        public static DbConnectionStringBuilder CreateConnectionStringBuilder(string connectionString)
        {
            return CreateConnectionStringBuilder(DetectFromConnectionString(connectionString), connectionString);
        }

        public static DbConnectionStringBuilder CreateConnectionStringBuilder(DB2iSeriesAdoProviderType dB2AdoProviderType, string connectionString)
        {
            if (dB2AdoProviderType == DB2iSeriesAdoProviderType.AccessClient)
                return DB2iSeriesTypes.CreateConnectionStringBuilder(connectionString);
            else if (dB2AdoProviderType == DB2iSeriesAdoProviderType.DB2Connect)
                return DB2Types.CreateConnectionStringBuilder(connectionString);
            else
                throw new NotSupportedException();
        }

        #endregion

        #region CreateConnection

        private static readonly Func<string, IDbConnection> ConnectionBuilder_ClientAccess = DynamicDataProviderBase.CreateConnectionExpression(GetConnectionType(DB2iSeriesAdoProviderType.AccessClient)).Compile();
        private static readonly Func<string, IDbConnection> ConnectionBuilder_DB2Connect = DynamicDataProviderBase.CreateConnectionExpression(GetConnectionType(DB2iSeriesAdoProviderType.DB2Connect)).Compile();

        public static IDbConnection CreateConnection(DB2iSeriesAdoProviderType adoProviderType, string connectionString)
        {
            if (adoProviderType == DB2iSeriesAdoProviderType.AccessClient)
                return ConnectionBuilder_ClientAccess(connectionString);
            else if (adoProviderType == DB2iSeriesAdoProviderType.DB2Connect)
                return ConnectionBuilder_DB2Connect(connectionString);
            else
                throw new NotSupportedException();
        }

        #endregion

        #region CreateDataConnection

        public static DataConnection CreateDataConnection(string connectionString, string providerName)
        {
            return new DataConnection(GetDataProvider(providerName), connectionString);
        }

        public static DataConnection CreateDataConnection(IDbConnection connection, string providerName)
        {
            return new DataConnection(GetDataProvider(providerName), connection);
        }

        public static DataConnection CreateDataConnection(IDbTransaction transaction, string providerName)
        {
            return new DataConnection(GetDataProvider(providerName), transaction);
        }

        #endregion

        #region BulkCopy

        public static BulkCopyType DefaultBulkCopyType = BulkCopyType.MultipleRows;

        public static BulkCopyRowsCopied MultipleRowsCopy<T>(DataConnection dataConnection, IEnumerable<T> source, int maxBatchSize = 1000, Action<BulkCopyRowsCopied> rowsCopiedCallback = null)
        {
            return dataConnection.BulkCopy(new BulkCopyOptions
            {
                BulkCopyType = BulkCopyType.MultipleRows,
                MaxBatchSize = maxBatchSize,
                RowsCopiedCallback = rowsCopiedCallback
            }, source);
        }

        public static BulkCopyRowsCopied ProviderSpecificBulkCopy<T>(DataConnection dataConnection, IEnumerable<T> source, int bulkCopyTimeout = 0, bool keepIdentity = false, int notifyAfter = 0, Action<BulkCopyRowsCopied> rowsCopiedCallback = null)
        {
            return dataConnection.BulkCopy(new BulkCopyOptions
            {
                BulkCopyType = BulkCopyType.ProviderSpecific,
                BulkCopyTimeout = bulkCopyTimeout,
                KeepIdentity = keepIdentity,
                NotifyAfter = notifyAfter,
                RowsCopiedCallback = rowsCopiedCallback
            }, source);
        }

        #endregion
    }
}