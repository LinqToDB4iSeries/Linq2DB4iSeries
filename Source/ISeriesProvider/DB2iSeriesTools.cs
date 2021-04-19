using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using LinqToDB.Common;
using LinqToDB.Configuration;
using LinqToDB.Data;
using System.Collections.Concurrent;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesTools
	{
		#region DataProvider instances

		private static readonly ConcurrentDictionary<string, DB2iSeriesDataProvider> dataProviders = new ConcurrentDictionary<string, DB2iSeriesDataProvider>();

		public static DB2iSeriesDataProvider GetDataProvider(string providerName)
		{
			if (!dataProviders.TryGetValue(providerName, out var dataProvider))
			{
				dataProvider = BuildDataProvider(providerName);
				dataProviders.TryAdd(providerName, dataProvider);
			}

			return dataProvider;
		}

		public static bool TryGetDataProvider(string providerName, out DB2iSeriesDataProvider dataProvider)
		{
			if (!DB2iSeriesProviderName.AllNames.Contains(providerName))
			{
				dataProvider = null;
				return false;
			}

			dataProvider = GetDataProvider(providerName);
			return true;
		}

		private static DB2iSeriesDataProvider BuildDataProvider(string providerName)
		{
			return new DB2iSeriesDataProvider(DB2iSeriesProviderName.GetProviderOptions(providerName));
		}

		public static DB2iSeriesDataProvider GetDataProvider(
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			DB2iSeriesMappingOptions mappingOptions)
		{
			return GetDataProvider(DB2iSeriesProviderName.GetProviderName(version, providerType, mappingOptions));
		}

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
					|| css.ProviderName.StartsWith(DB2iSeriesProviderName.DB2)
#if NETFRAMEWORK
					|| css.ProviderName == DB2iSeriesAccessClientProviderAdapter.AssemblyName
#endif
					))
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

		public static DataConnection CreateDataConnection(
			string connectionString, 
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString)
		{
			return new DataConnection(GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString)), connectionString);
		}

		public static DataConnection CreateDataConnection(
			IDbConnection connection, 
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString)
		{
			return new DataConnection(GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString)), connection);
		}

		public static DataConnection CreateDataConnection(
			IDbTransaction transaction,
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString)
		{
			return new DataConnection(GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString)), transaction);
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

		#region Configuration Helpers

		/// <summary>
		/// Configure connection to use specific iSeries provider and connection string.
		/// </summary>
		/// <param name="builder">Instance of <see cref="LinqToDbConnectionOptionsBuilder"/>.</param>
		/// <param name="provider">iSeries provider to use.</param>
		/// <param name="connectionString">iSeries connection string.</param>
		/// <returns>The builder instance so calls can be chained.</returns>
		public static LinqToDbConnectionOptionsBuilder UseDB2iSeries(this LinqToDbConnectionOptionsBuilder builder, DB2iSeriesDataProvider provider, string connectionString)
		{
			return builder.UseConnectionString(provider, connectionString);
		}

		#endregion
	}
}
