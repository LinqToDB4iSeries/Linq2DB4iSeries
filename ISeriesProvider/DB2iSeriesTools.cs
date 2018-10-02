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

    public static partial class DB2iSeriesTools
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
            //Caution - This is not run at all if the type is not referenced, meaning that an application that starts with new DataConnection(configuration)
            //will never get the detector running. Perhaps it makes more sense to move this to an EnableAutoDetection static method instead of a static bool property
            AutoDetectProvider = true;
            DataConnection.AddProviderDetector(ProviderDetector);

            //Caution - When using DataConnection.GetDataProvider the default provider names will resolve to iDB2 implementation with the current implementation 
            //Chaning the DefaultAdoProviderType will only be used fo the DB2iSeriesTools.GetDataProvider
            //Possible solutions would be to move registration to a static method with the default provider as parameter (and error on second call)
            //Another solution is to make the propery private const (in essense turn it to a compile time option)
            foreach (var item in dataProviders)
                DataConnection.AddDataProvider(item.Key, item.Value);
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

        public static string GetSqlObjectDelimiter(DB2iSeriesNamingConvention namingConvention)
        {
            return namingConvention == DB2iSeriesNamingConvention.Sql ? "." : "/";
        }

        public static string GetSqlObjectDelimiter(string connectionString)
        {
            return GetSqlObjectDelimiter(GetNamingConvention(connectionString));
        }

        public static string GetSqlObjectDelimiter(IDbConnection dbConnection)
        {
            return GetSqlObjectDelimiter(GetNamingConvention(dbConnection));
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
                throw new LinqToDBException("Unsupported provider type");
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
            //Global configurations should work as well
            //Linq2db has this commented as well 
            //if (css.IsGlobal)
            //    return null;

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


                    using (var conn = CreateConnection(providerType, cs))
                    {
                        conn.Open();

                        var version = GetServerVersion(conn);
                        var maxPtfLevel = GetLevel(conn, version.PtfGroupName);
                        var minLevel = GetMinLevel(version, maxPtfLevel);

                        switch (minLevel)
                        {
                            case DB2iSeriesLevels.V7_1_38:
                                return GetDataProvider(DB2iSeriesProviderName.GetFromOptions(new DB2iSeriesDataProviderOptions(DB2iSeriesLevels.V7_1_38, false, providerType)));
                            default:
                                return GetDataProvider(DB2iSeriesProviderName.GetFromOptions(new DB2iSeriesDataProviderOptions(DB2iSeriesLevels.Any, false, providerType)));
                        }
                    }
                }
                catch (Exception)
                {
                    //Error or return default provider?
                    return null;
                }
            }

            //Error or return default provider?
            return null;
        }

        public static DB2iSeriesLevels GetMinLevel(DB2iSeriesServerVersion version, int maxPtfGroupLevel)
        {
            if (version.Major < 7)
                return DB2iSeriesLevels.Any;

            if (version.Major > 7)
                return DB2iSeriesLevels.V7_1_38;

            switch (version.Minor)
            {
                case 1:
                    return maxPtfGroupLevel < 38 ? DB2iSeriesLevels.Any : DB2iSeriesLevels.V7_1_38;
                case 2:
                    return maxPtfGroupLevel < 9 ? DB2iSeriesLevels.Any : DB2iSeriesLevels.V7_1_38;
                //case 3:
                default:
                    return DB2iSeriesLevels.V7_1_38;
            }
        }

        public static DB2iSeriesServerVersion GetServerVersion(IDbConnection dbConnection)
        {
            var providerType = GetAdoProviderType(dbConnection);
            switch (providerType)
            {
                case DB2iSeriesAdoProviderType.AccessClient:
                    if (MemberAccessor.TryGetValue<string>(dbConnection, "ServerVersion", out var serverVersion))
                    {
                        var serverVersionParts = serverVersion.Split('.');
                        var major = int.Parse(serverVersionParts[0]);
                        var minor = int.Parse(serverVersionParts[1]);
                        var ptf = GetPtfGroupName(major, minor);
                        return new DB2iSeriesServerVersion(major, minor, ptf);
                    }
                    throw new LinqToDBException("ServerVersion not found in iDB2Connection");
                case DB2iSeriesAdoProviderType.DB2Connect:
                    if (MemberAccessor.TryGetValue<int>(dbConnection, "ServerMajorVersion", out var serverMajorVersion)
                        && MemberAccessor.TryGetValue<int>(dbConnection, "ServerMinorVersion", out var serverMinorVersion))
                    {
                        var ptf = GetPtfGroupName(serverMajorVersion, serverMinorVersion);
                        return new DB2iSeriesServerVersion(serverMajorVersion, serverMinorVersion, ptf);
                    }
                    throw new LinqToDBException("ServerMajorVersion or ServerMinorVersion not found in DB2Connection");
                default:
                    throw new LinqToDBException($"Unsupported provider type {providerType.ToString()}");
            }
        }

        private static string GetPtfGroupName(int major, int minor)
        {
            return string.Format("SF99{0:D1}{1:D2}", major, minor);
        }

        private static int GetLevel(IDbConnection conn, string ptf)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    $"SELECT MAX(PTF_GROUP_LEVEL) FROM QSYS2{DB2iSeriesTools.GetSqlObjectDelimiter(conn)}GROUP_PTF_INFO WHERE PTF_GROUP_NAME = @p1 AND PTF_GROUP_STATUS = 'INSTALLED'";
                var param = cmd.CreateParameter();
                param.ParameterName = "p1";
                param.Value = ptf;

                cmd.Parameters.Add(param);

                return Converter.ChangeTypeTo<int>(cmd.ExecuteScalar());
            }
        }

        #endregion

        #region OnInitialized

        //This whole section existed for test cases to run after the Types were plugged into the TypeCreators
        //It is obsolete in the current implementation - left in for compatibility only - discard if possible

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

        #region ConnectionString Tools

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

        private static readonly Lazy<Func<string, IDbConnection>> ConnectionBuilder_ClientAccess
            = new Lazy<Func<string, IDbConnection>>(() => DynamicDataProviderBase.CreateConnectionExpression(GetConnectionType(DB2iSeriesAdoProviderType.AccessClient)).Compile());
        private static readonly Lazy<Func<string, IDbConnection>> ConnectionBuilder_DB2Connect
            = new Lazy<Func<string, IDbConnection>>(() => DynamicDataProviderBase.CreateConnectionExpression(GetConnectionType(DB2iSeriesAdoProviderType.DB2Connect)).Compile());

        public static IDbConnection CreateConnection(DB2iSeriesAdoProviderType adoProviderType, string connectionString)
        {
            if (adoProviderType == DB2iSeriesAdoProviderType.AccessClient)
                return ConnectionBuilder_ClientAccess.Value(connectionString);
            else if (adoProviderType == DB2iSeriesAdoProviderType.DB2Connect)
                return ConnectionBuilder_DB2Connect.Value(connectionString);
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

        public static BulkCopyType DefaultBulkCopyType { get; set; } = BulkCopyType.MultipleRows;

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