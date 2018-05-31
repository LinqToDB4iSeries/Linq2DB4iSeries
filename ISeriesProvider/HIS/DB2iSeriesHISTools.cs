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
    using System.IO;
    using System.Linq;
	using System.Linq.Expressions;

	using Configuration;
	using Data;

	public static class DB2iSeriesHISTools
    {
		public static string AssemblyName = "Microsoft.HostIntegration.MsDb2Client";
		public static string ConnectionTypeName = AssemblyName + ".MsDb2Connection, " + AssemblyName;
		public static string DataReaderTypeName = AssemblyName + ".MsDb2DataReader, " + AssemblyName;
        public static string DbParameterTypeName = AssemblyName + ".MsDb2Parameter, " + AssemblyName;
        public static string DbTypeTypeName = AssemblyName + ".MsDb2Type, " + AssemblyName;

        static readonly DB2iSeriesHISDataProvider _db2iDataProvider = new DB2iSeriesHISDataProvider(DB2iSeriesHISProviderName.DB2, DB2iSeriesLevels.Any, false);
        static readonly DB2iSeriesHISDataProvider _db2iDataProvider_gas = new DB2iSeriesHISDataProvider(DB2iSeriesHISProviderName.DB2_GAS, DB2iSeriesLevels.Any, true);
        static readonly DB2iSeriesHISDataProvider _db2iDataProvider_73 = new DB2iSeriesHISDataProvider(DB2iSeriesHISProviderName.DB2_73, DB2iSeriesLevels.V7_1_38, false);
        static readonly DB2iSeriesHISDataProvider _db2iDataProvider_73_gas = new DB2iSeriesHISDataProvider(DB2iSeriesHISProviderName.DB2_73_GAS, DB2iSeriesLevels.V7_1_38, true);

        public static bool AutoDetectProvider { get; set; }

        static DB2iSeriesHISTools()
        {
            AutoDetectProvider = true;
            DataConnection.AddDataProvider(DB2iSeriesHISProviderName.DB2, _db2iDataProvider);
            DataConnection.AddDataProvider(DB2iSeriesHISProviderName.DB2_GAS, _db2iDataProvider_gas);
            DataConnection.AddDataProvider(DB2iSeriesHISProviderName.DB2_73, _db2iDataProvider_73);
            DataConnection.AddDataProvider(DB2iSeriesHISProviderName.DB2_73_GAS, _db2iDataProvider_73_gas);
            DataConnection.AddProviderDetector(ProviderDetector);
        }

        private static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
        {
            if (css.IsGlobal)
            {
                return null;
            }

            if (DB2iSeriesProviderName.AllNames.Contains(css.Name) || new[] { DB2iSeriesProviderName.DB2, AssemblyName }.Contains(css.ProviderName))
            {
                if (AutoDetectProvider)
                {
                    try
                    {
                        var connectionType = Type.GetType(ConnectionTypeName, true);
                        var connectionCreator = DynamicDataProviderBase.CreateConnectionExpression(connectionType).Compile();
                        var cs = string.IsNullOrWhiteSpace(connectionString) ? css.ConnectionString : connectionString;

                        using (var conn = connectionCreator(cs))
                        {
                            conn.Open();

                            return GetDataProvider(css.Name);
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            return null;
        }

        public static IDataProvider GetDataProvider(string name)
        {
            switch (name)
            {
                case DB2iSeriesHISProviderName.DB2_73: return _db2iDataProvider_73;
                case DB2iSeriesHISProviderName.DB2_GAS: return _db2iDataProvider_gas;
                case DB2iSeriesHISProviderName.DB2_73_GAS: return _db2iDataProvider_73_gas;
                default: return _db2iDataProvider;
            }
        }

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