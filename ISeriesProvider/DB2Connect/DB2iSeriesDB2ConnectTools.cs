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

	public static class DB2iSeriesDB2ConnectTools
    {
		public static string AssemblyName = "IBM.Data.DB2";
		public static string ConnectionTypeName = AssemblyName + ".DB2Connection, " + AssemblyName;
		public static string DataReaderTypeName = AssemblyName + ".DB2DataReader, " + AssemblyName;
		public const string IdentityColumnSql = "identity_val_local()";
        public static bool IsCore;

		static readonly DB2iSeriesDB2ConnectDataProvider _db2iSeriesDataProvider = new DB2iSeriesDB2ConnectDataProvider();

		public static bool AutoDetectProvider { get; set; }

		static DB2iSeriesDB2ConnectTools()
		{
            try
            {
                var path = typeof(DB2iSeriesDB2ConnectTools).AssemblyEx().GetPath();

                IsCore = File.Exists(Path.Combine(path, (AssemblyName = "IBM.Data.DB2.Core") + ".dll"));

                if (!IsCore)
                    AssemblyName = "IBM.Data.DB2";
            }
            catch (Exception)
            {
            }

            AutoDetectProvider = true;
			DataConnection.AddDataProvider(DB2iSeriesDB2ConnectFactory.ProviderName, _db2iSeriesDataProvider);
			DataConnection.AddDataProvider(_db2iSeriesDataProvider);
			DataConnection.AddProviderDetector(ProviderDetector);
		}

		private static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
		{
			if (css.IsGlobal)
			{
				return null;
			}
			if (css.Name == DB2iSeriesDB2ConnectFactory.ProviderName || new[] { DB2iSeriesDB2ConnectFactory.ProviderName, AssemblyName }.Contains(css.ProviderName))
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
							return _db2iSeriesDataProvider;
						}
					}
					catch (Exception)
					{
					}
				}
			}
			return null;
		}

		public static IDataProvider GetDataProvider()
		{
			return _db2iSeriesDataProvider;
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

		public static DataConnection CreateDataConnection(string connectionString)
		{
			return new DataConnection(_db2iSeriesDataProvider, connectionString);
		}

		public static DataConnection CreateDataConnection(IDbConnection connection)
		{
			return new DataConnection(_db2iSeriesDataProvider, connection);
		}

		public static DataConnection CreateDataConnection(IDbTransaction transaction)
		{
			return new DataConnection(_db2iSeriesDataProvider, transaction);
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