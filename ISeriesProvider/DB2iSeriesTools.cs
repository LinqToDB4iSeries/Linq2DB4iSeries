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
    using Configuration;
    using Data;

    public static class DB2iSeriesTools
    {
        #region Public Settings

        public static bool AutoDetectProvider { get; set; }
        public static DB2iSeriesAdoProviderType DefaultAdoProviderType { get; set; } = DB2iSeriesAdoProviderType.AccessClient;

        #endregion

        #region Type Helpers

        public static string GetProviderSpecificTypeNamespace(DB2iSeriesAdoProviderType adoProviderType)
        {
            switch (adoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DB2Types.TypesNamespaceName;
                //case DB2iSeriesAdoProviderType.AccessClient:
                default:
                    return DB2iSeriesTypes.TypesNamespaceName;
            }
        }

        public static string GetConnectionTypeName(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            switch (dB2AdoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DB2Types.ConnectionTypeName;
                //case DB2iSeriesAdoProviderType.AccessClient:
                default:
                    return DB2iSeriesTypes.ConnectionTypeName;
            }
        }

        public static string GetDataReaderTypeName(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            switch (dB2AdoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DB2Types.DataReaderTypeName;
                //case DB2iSeriesAdoProviderType.AccessClient:
                default:
                    return DB2iSeriesTypes.DataReaderTypeName;
            }
        }

        public static string GetConnectionNamespace(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            switch (dB2AdoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DB2Types.NamespaceName;
                //case DB2iSeriesAdoProviderType.AccessClient:
                default:
                    return DB2iSeriesTypes.NamespaceName;
            }
        }

        public static Type GetConnectionType(DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            switch (dB2AdoProviderType)
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DB2Types.ConnectionType;
                //case DB2iSeriesAdoProviderType.AccessClient:
                default:
                    return DB2iSeriesTypes.ConnectionType;
            }
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
        public static string GetDB2DummyTableName(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.Sql)
        {
            var seperator = (naming == DB2iSeriesNamingConvention.System) ? "/" : ".";
            return string.Format("SYSIBM{0}SYSDUMMY1", seperator);
        }

        public static string GetDB2DummyTableName(IDbConnection dbConnection)
        {
            return GetDB2DummyTableName(GetNamingConvention(dbConnection));
        }

        #endregion

        #region DataProvider Cache

        //All instances of unique data providers
        static readonly IReadOnlyDictionary<string, DB2iSeriesDataProvider> actualDataProviders = 
            DB2iSeriesProviderName.AllNames
                .Where(x => !DB2iSeriesProviderName.IsVirtual(x))
                .Select(x => new DB2iSeriesDataProvider(x))
            .ToDictionary(x => x.Name);

        //All data providers, including virtual/mapped ones
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

        private static DB2iSeriesAdoProviderType DetectFromConnection(IDbConnection dbConnection)
        {
            if (dbConnection.GetType() == DB2iSeriesTypes.ConnectionType)
                return DB2iSeriesAdoProviderType.AccessClient;
            if (dbConnection.GetType() == DB2Types.ConnectionType)
                return DB2iSeriesAdoProviderType.DB2Connect;
            else
                throw new LinqToDBException("Unsopported provider type");
        }

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

            if (css.ProviderName == DB2iSeriesTypes.AssemblyName)
                return GetDataProvider(DB2iSeriesProviderName.DB2iSeries_AccessClient);
            else if (css.ProviderName == DB2Types.AssemblyName_Net || css.ProviderName == DB2Types.AssemblyName_Core)
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

        public static DB2iSeriesNamingConvention GetNamingConvention(IDbConnection dbConnection)
        {
            switch (DetectFromConnection(dbConnection))
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DB2iSeriesNamingConvention.Sql;
                //case DB2iSeriesAdoProviderType.AccessClient:
                default:
                    if (!MemberAccessor.TryGetValue<int>(dbConnection, "Naming", out var naming))
                        return naming == 1 ? DB2iSeriesNamingConvention.System : DB2iSeriesNamingConvention.Sql;

                    return GetNamingConvetionFromConnectionString_AccessClient(dbConnection.ConnectionString);
            }
        }

        public static DB2iSeriesNamingConvention GetNamingConvention(string connectionString)
        {
            switch (DetectFromConnectionString(connectionString))
            {
                case DB2iSeriesAdoProviderType.DB2Connect:
                    return DB2iSeriesNamingConvention.Sql;
                //case DB2iSeriesAdoProviderType.AccessClient:
                default:
                    return GetNamingConvetionFromConnectionString_AccessClient(connectionString);
            }
        }

        private static DB2iSeriesNamingConvention GetNamingConvetionFromConnectionString_AccessClient(string connectionString)
        {
            var csb = DB2iSeriesTypes.CreateConnectionStringBuilder(connectionString);
            return csb["Naming"]?.ToString() == "1" ? DB2iSeriesNamingConvention.System : DB2iSeriesNamingConvention.Sql;
        }

        public static string GetSqlObjectDelimiter(string connectionString)
        {
            return GetNamingConvention(connectionString) == DB2iSeriesNamingConvention.Sql ? "." : "/";
        }

        public static string GetSqlObjectDelimiter(IDbConnection dbConnection)
        {
            return GetNamingConvention(dbConnection) == DB2iSeriesNamingConvention.Sql ? "." : "/";
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