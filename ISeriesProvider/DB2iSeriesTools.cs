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

		#region AutoDetection

		public static bool AutoDetectProvider { get; set; } = true;

		public static void RegisterProviderDetector()
		{
			DataConnection.AddProviderDetector(ProviderDetector);
		}

		private static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
		{
			if (css.IsGlobal)
			{
				return null;
			}

			if (DB2iSeriesProviderName.AllNames.Contains(css.Name) || new[] { DB2iSeriesProviderName.DB2, DB2iSeriesProviderAdapter.AssemblyName }.Contains(css.ProviderName))
			{
				return css.Name switch
				{
					DB2iSeriesProviderName.DB2 => _db2iDataProvider.Value,
					DB2iSeriesProviderName.DB2_73 => _db2iDataProvider_73.Value,
					DB2iSeriesProviderName.DB2_GAS => _db2iDataProvider_gas.Value,
					DB2iSeriesProviderName.DB2_73_GAS => _db2iDataProvider_73_gas.Value,
					_ => AutoDetectDataProvider(
							AutoDetectProvider ?
							string.IsNullOrWhiteSpace(connectionString) ? css.ConnectionString : connectionString :
							(string)null)
				};
			}
			return null;
		}

		private static DB2iSeriesDataProvider AutoDetectDataProvider(string connectionString)
		{
			try
			{
				var minLevel = GetServerMinLevel(connectionString);

				return minLevel switch
				{
					DB2iSeriesLevels.V7_1_38 => _db2iDataProvider_73.Value,
					_ => _db2iDataProvider.Value,
				};
			}
			catch(Exception e)
			{
				throw new LinqToDBException($"Failed to detect DB2iSeries provider from given string: {connectionString}. Error: {e.Message}");
			}
		}

		private static DB2iSeriesLevels GetServerMinLevel(string connectionString)
		{
			using (var conn = DB2iSeriesProviderAdapter.GetInstance().CreateConnection(connectionString))
			{
				conn.Open();

				var serverVersionParts = conn.ServerVersion.Substring(0, 5).Split('.');
				var major = int.Parse(serverVersionParts.First());
				var minor = int.Parse(serverVersionParts.Last());

				if (major > 7 || minor > 2)
					return DB2iSeriesLevels.V7_1_38;

				if (major == 7
					&& (minor == 1 || minor == 2))
				{
					var patchLevel = GetMaxPatchLevel(conn, major, minor);
					if ((minor == 1 && patchLevel > 38)
						|| (minor == 2 && patchLevel > 9))
						return DB2iSeriesLevels.V7_1_38;
				}

				return DB2iSeriesLevels.Any;
			}
		}

		private static int GetMaxPatchLevel(DB2iSeriesProviderAdapter.iDB2Connection connection, int major, int minor)
		{
			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText =
					"SELECT MAX(PTF_GROUP_LEVEL) FROM QSYS2.GROUP_PTF_INFO WHERE PTF_GROUP_NAME = @p1 AND PTF_GROUP_STATUS = 'INSTALLED'";
				var param = cmd.CreateParameter();
				param.ParameterName = "p1";
				param.Value = $"SF99{major:D1}{minor:D2}";

				cmd.Parameters.Add(param);

				return Converter.ChangeTypeTo<int>(cmd.ExecuteScalar());
			}
		}

		#endregion

		#region CreateDataConnection

		public static IDataProvider GetDataProvider(string providerName)
		{
			return providerName switch
			{
				DB2iSeriesProviderName.DB2 => _db2iDataProvider.Value,
				DB2iSeriesProviderName.DB2_73 => _db2iDataProvider_73.Value,
				DB2iSeriesProviderName.DB2_GAS => _db2iDataProvider_gas.Value,
				DB2iSeriesProviderName.DB2_73_GAS => _db2iDataProvider_73_gas.Value,
				_ => throw new ArgumentOutOfRangeException(nameof(providerName), $"'{providerName}' is not a valid DB2iSeries provider name.")
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
