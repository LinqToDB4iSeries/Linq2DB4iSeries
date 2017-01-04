using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using LinqToDB.Extensions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using System.Linq;
	using System.Linq.Expressions;

	using Configuration;
	using Data;

	public static class DB2iSeriesTools
	{
		public const string AssemblyName = "IBM.Data.DB2.iSeries";
		public const string ConnectionTypeName = AssemblyName + ".iDB2Connection, " + AssemblyName;
		public const string DataReaderTypeName = AssemblyName + ".iDB2DataReader, " + AssemblyName;
		public const string IdentityColumnSql = "identity_val_local()";

		public static string iSeriesDummyTableName(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.System)
		{
			var seperator = (naming == DB2iSeriesNamingConvention.System) ? "/" : ".";
			return string.Format("SYSIBM{0}SYSDUMMY1", seperator);
		}

		static readonly DB2iSeriesDataProvider _db2iSeriesDataProvider = new DB2iSeriesDataProvider();

		public static bool AutoDetectProvider { get; set; }

		static DB2iSeriesTools()
		{
			AutoDetectProvider = true;
			DataConnection.AddDataProvider(DB2iSeriesFactory.ProviderName, _db2iSeriesDataProvider);
			DataConnection.AddDataProvider(_db2iSeriesDataProvider);
			DataConnection.AddProviderDetector(ProviderDetector);
		}

		private static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
		{
			if (css.IsGlobal)
			{
				return null;
			}
			if (css.Name == DB2iSeriesFactory.ProviderName || new[] { DB2iSeriesFactory.ProviderName, AssemblyName }.Contains(css.ProviderName))
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
					catch (Exception ex)
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