using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using LinqToDB.Common;
using LinqToDB.Configuration;
using LinqToDB.Data;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesTools
	{
		#region DataProvider instances

		static readonly Lazy<DB2iSeriesDataProvider> _db2iDataProvider = new Lazy<DB2iSeriesDataProvider>(() =>
		{
			var provider = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false);
			DataConnection.AddDataProvider(provider);
			return provider;
		});

		static readonly Lazy<DB2iSeriesDataProvider> _db2iDataProvider_gas = new Lazy<DB2iSeriesDataProvider>(() =>
		{
			var provider = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_GAS, DB2iSeriesLevels.Any, true);
			DataConnection.AddDataProvider(provider);
			return provider;
		});

		static readonly Lazy<DB2iSeriesDataProvider> _db2iDataProvider_73 = new Lazy<DB2iSeriesDataProvider>(() =>
		{
			var provider = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73, DB2iSeriesLevels.V7_1_38, false);
			DataConnection.AddDataProvider(provider);
			return provider;
		});

		static readonly Lazy<DB2iSeriesDataProvider> _db2iDataProvider_73_gas = new Lazy<DB2iSeriesDataProvider>(() =>
		{
			var provider = new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_73_GAS, DB2iSeriesLevels.V7_1_38, true);
			DataConnection.AddDataProvider(provider);
			return provider;
		});

		static readonly Lazy<DB2iSeriesODBCDataProvider> _db2iOdbcDataProvider = new Lazy<DB2iSeriesODBCDataProvider>(() =>
		{
			var provider = new DB2iSeriesODBCDataProvider(DB2iSeriesProviderName.DB2_ODBC, DB2iSeriesLevels.Any, false);
			DataConnection.AddDataProvider(provider);
			return provider;
		});

		static readonly Lazy<DB2iSeriesOleDbDataProvider> _db2iOleDbDataProvider = new Lazy<DB2iSeriesOleDbDataProvider>(() =>
		{
			var provider = new DB2iSeriesOleDbDataProvider(DB2iSeriesProviderName.DB2_ODBC, DB2iSeriesLevels.Any, false);
			DataConnection.AddDataProvider(provider);
			return provider;
		});

		static readonly Lazy<DB2iSeriesDB2DataProvider> _db2iDB2DataProvider = new Lazy<DB2iSeriesDB2DataProvider>(() =>
		{
			var provider = new DB2iSeriesDB2DataProvider(DB2iSeriesProviderName.DB2_DB2Connect, DB2iSeriesLevels.Any, false);
			DataConnection.AddDataProvider(provider);
			return provider;
		});

		#endregion

		#region AutoDetection

		private readonly static DB2iSeriesProviderDetector providerDetector = new DB2iSeriesProviderDetector();

		public static bool AutoDetectProvider { get; set; } = true;

		public static void RegisterProviderDetector()
		{
			DataConnection.AddProviderDetector(ProviderDetector);
		}

		private static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
		{
			if (!css.IsGlobal 
				&& (DB2iSeriesProviderName.AllNames.Contains(css.Name) 
					|| css.ProviderName == DB2iSeriesProviderName.DB2 
					|| css.ProviderName == DB2iSeriesProviderAdapter.AssemblyName))
			{
				if (TryGetDataProvider(css.Name, out var dataProvider))
					return dataProvider;

				if (AutoDetectProvider)

					return providerDetector.AutoDetectDataProvider(
								string.IsNullOrWhiteSpace(connectionString) ? css.ConnectionString : connectionString);
			}

			return null;
		}

		#endregion

		#region CreateDataConnection

		public static bool TryGetDataProvider(string providerName, out IDB2iSeriesDataProvider dataProvider)
		{
			dataProvider = providerName switch
			{
				DB2iSeriesProviderName.DB2 => _db2iDataProvider.Value,
				DB2iSeriesProviderName.DB2_73 => _db2iDataProvider_73.Value,
				DB2iSeriesProviderName.DB2_GAS => _db2iDataProvider_gas.Value,
				DB2iSeriesProviderName.DB2_73_GAS => _db2iDataProvider_73_gas.Value,
				DB2iSeriesProviderName.DB2_ODBC => _db2iOdbcDataProvider.Value,
				DB2iSeriesProviderName.DB2_OleDb => _db2iOleDbDataProvider.Value,
				DB2iSeriesProviderName.DB2_DB2Connect => _db2iDB2DataProvider.Value,
				_ => null
			};

			return dataProvider != null;
		}

		public static IDB2iSeriesDataProvider GetDataProvider(string providerName)
		{
			if (TryGetDataProvider(providerName, out var dataProvider))
				return dataProvider;

			throw new ArgumentOutOfRangeException(nameof(providerName), $"'{providerName}' is not a valid DB2iSeries provider name.");
		}

		public static IDataProvider GetDataProvider(DB2iSeriesAdoProviderType providerType)
		{
			return providerType switch
			{
				DB2iSeriesAdoProviderType.AccessClient => _db2iDataProvider.Value,
				DB2iSeriesAdoProviderType.Odbc => _db2iOdbcDataProvider.Value,
				DB2iSeriesAdoProviderType.OleDb => _db2iOleDbDataProvider.Value,
				DB2iSeriesAdoProviderType.DB2 => _db2iDB2DataProvider.Value,
				_ => throw new ArgumentOutOfRangeException(nameof(providerType), $"'{providerType}' is not a valid DB2iSeries ado provider type.")
			};
		}

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

		[Obsolete("Please use the BulkCopy extension methods within DataConnectionExtensions")]
		public static BulkCopyRowsCopied MultipleRowsCopy<T>(DataConnection dataConnection,
			IEnumerable<T> source,
			int maxBatchSize = 1000,
			Action<BulkCopyRowsCopied> rowsCopiedCallback = null) where T : class
		{
			return dataConnection.BulkCopy(new BulkCopyOptions
			{
				BulkCopyType = BulkCopyType.MultipleRows,
				MaxBatchSize = maxBatchSize,
				RowsCopiedCallback = rowsCopiedCallback
			}, source);
		}

		[Obsolete("Please use the BulkCopy extension methods within DataConnectionExtensions")]
		public static BulkCopyRowsCopied ProviderSpecificBulkCopy<T>(DataConnection dataConnection,
			IEnumerable<T> source,
			int bulkCopyTimeout = 0,
			bool keepIdentity = false,
			int notifyAfter = 0,
			Action<BulkCopyRowsCopied> rowsCopiedCallback = null) where T : class
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
