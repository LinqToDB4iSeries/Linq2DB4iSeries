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
		public const string MapGuidAsString = "MapGuidAsString";

		public static string iSeriesDummyTableName(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.System)
		{
			var seperator = (naming == DB2iSeriesNamingConvention.System) ? "/" : ".";
			return string.Format("SYSIBM{0}SYSDUMMY1", seperator);
		}

		static readonly DB2iSeriesDataProvider _db2iDataProvider = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false);
		static readonly DB2iSeriesDataProvider _db2iDataProvider_gas = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_GAS, DB2iSeriesLevels.Any, true);
		static readonly DB2iSeriesDataProvider _db2iDataProvider_73 = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73, DB2iSeriesLevels.V7_1_38, false);
		static readonly DB2iSeriesDataProvider _db2iDataProvider_73_gas = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73_GAS, DB2iSeriesLevels.V7_1_38, true);

		public static bool AutoDetectProvider { get; set; }

		static DB2iSeriesTools()
		{
			AutoDetectProvider = true;
			DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2, _db2iDataProvider);
			DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2_GAS, _db2iDataProvider_gas);
			DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2_73, _db2iDataProvider_73);
			DataConnection.AddDataProvider(DB2iSeriesProviderName.DB2_73_GAS, _db2iDataProvider_73_gas);
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

							return GetDataProvider(css.Name, connectionString);
						}
					}
					catch (Exception)
					{
					}
				}
			}
			return null;
		}

		public static IDataProvider GetDataProvider(string name, string connectionString)
		{
			switch (name)
			{
				case DB2iSeriesProviderName.DB2_73: return _db2iDataProvider_73;
				case DB2iSeriesProviderName.DB2_GAS: return _db2iDataProvider_gas;
				case DB2iSeriesProviderName.DB2_73_GAS: return _db2iDataProvider_73_gas;
				default:
					if (string.IsNullOrWhiteSpace(connectionString))
						return _db2iDataProvider;

					var parts = connectionString.Split(';').ToList();
					var gas = parts.FirstOrDefault(p => p.Trim().ToLower().StartsWith("mapguidasstring"));
					var minVer = parts.FirstOrDefault(p => p.Trim().ToLower().StartsWith("minver"));

					var isGas = gas != null && gas.EndsWith("true", StringComparison.CurrentCultureIgnoreCase);
					var level = minVer != null && 
					            (minVer.EndsWith("7.1.38", StringComparison.CurrentCultureIgnoreCase) || 
					             minVer.EndsWith("7.2", StringComparison.CurrentCultureIgnoreCase) || 
					             minVer.EndsWith("7.3", StringComparison.CurrentCultureIgnoreCase)) ? 
						DB2iSeriesLevels.V7_1_38 : 
						DB2iSeriesLevels.Any;

					if (isGas && level == DB2iSeriesLevels.V7_1_38)
						return _db2iDataProvider_73_gas;
					if (isGas)
						return _db2iDataProvider_gas;
					if (level == DB2iSeriesLevels.V7_1_38)
						return _db2iDataProvider_73;

					return _db2iDataProvider;
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
			return new DataConnection(GetDataProvider(providerName, connectionString), connectionString);
		}

		public static DataConnection CreateDataConnection(IDbConnection connection, string providerName)
		{
			return new DataConnection(GetDataProvider(providerName, string.Empty), connection);
		}

		public static DataConnection CreateDataConnection(IDbTransaction transaction, string providerName)
		{
			return new DataConnection(GetDataProvider(providerName, string.Empty), transaction);
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