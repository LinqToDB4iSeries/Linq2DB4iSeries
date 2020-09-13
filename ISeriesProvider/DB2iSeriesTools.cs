using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using LinqToDB.Common;
using LinqToDB.Extensions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using System.Linq;
	using System.Linq.Expressions;
	using System.Reflection;
	using Configuration;
	using Data;

	public static class DB2iSeriesTools
	{
		public const string IdentityColumnSql = "identity_val_local()";
		public const string MapGuidAsString = "MapGuidAsString";

		public static string iSeriesDummyTableName(DB2iSeriesNamingConvention naming = DB2iSeriesNamingConvention.System)
		{
			var seperator = (naming == DB2iSeriesNamingConvention.System) ? "/" : ".";
			return string.Format("SYSIBM{0}SYSDUMMY1", seperator);
		}

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

		public static bool AutoDetectProvider { get; set; } = true;

		private static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
		{
			if (css.IsGlobal)
			{
				return null;
			}


			if (DB2iSeriesProviderName.AllNames.Contains(css.Name) || new[] { DB2iSeriesProviderName.DB2, DB2iSeriesProviderAdapter.AssemblyName }.Contains(css.ProviderName))
			{
				switch (css.Name)
				{
					case DB2iSeriesProviderName.DB2_73: return _db2iDataProvider_73.Value;
					case DB2iSeriesProviderName.DB2_GAS: return _db2iDataProvider_gas.Value;
					case DB2iSeriesProviderName.DB2_73_GAS: return _db2iDataProvider_73_gas.Value;
				}

				if (AutoDetectProvider)
				{
					try
					{

						var cs = string.IsNullOrWhiteSpace(connectionString) ? css.ConnectionString : connectionString;

						using (var conn = DB2iSeriesProviderAdapter.GetInstance().CreateConnection(cs))
						{
							conn.Open();

							var serverVersion = conn.ServerVersion.Substring(0, 5);

							string ptf;
							int desiredLevel;

							switch (serverVersion)
							{
								case "07.03":
									return _db2iDataProvider_73.Value;
								case "07.02":
									ptf = "SF99702";
									desiredLevel = 9;
									break;
								case "07.01":
									ptf = "SF99701";
									desiredLevel = 38;
									break;
								default:
									return _db2iDataProvider.Value;
							}

							using (var cmd = conn.CreateCommand())
							{
								cmd.CommandText =
									"SELECT MAX(PTF_GROUP_LEVEL) FROM QSYS2.GROUP_PTF_INFO WHERE PTF_GROUP_NAME = @p1 AND PTF_GROUP_STATUS = 'INSTALLED'";
								var param = cmd.CreateParameter();
								param.ParameterName = "p1";
								param.Value = ptf;

								cmd.Parameters.Add(param);

								var level = Converter.ChangeTypeTo<int>(cmd.ExecuteScalar());

								return level < desiredLevel ? _db2iDataProvider.Value : _db2iDataProvider_73.Value;
							}
						}
					}
					catch
					{
					}
				}
			}
			return null;
		}

		#region CreateDataConnection

		private static IDataProvider GetDataProvider(string providerName)
		{
			switch (providerName)
			{

				case DB2iSeriesProviderName.DB2_73: return _db2iDataProvider_73.Value;

				case DB2iSeriesProviderName.DB2_GAS: return _db2iDataProvider_gas.Value;

				case DB2iSeriesProviderName.DB2_73_GAS: return _db2iDataProvider_73_gas.Value;

				default: return _db2iDataProvider.Value;

			}

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
