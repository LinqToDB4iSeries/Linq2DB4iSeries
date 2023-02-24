﻿using System;
using System.Linq;
using System.Collections.Generic;
using LinqToDB.Configuration;
using LinqToDB.Data;
using System.Collections.Concurrent;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesTools
	{
		#region DataProvider instances

		private static readonly ConcurrentDictionary<string, DB2iSeriesDataProvider> dataProviders = new();

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

		public static IDataProvider ProviderDetector(ConnectionOptions connectionOptions)
		{
			if (DB2iSeriesProviderName.AllNames.Contains(connectionOptions.ConfigurationString)
					|| connectionOptions.ProviderName.StartsWith(DB2iSeriesProviderName.DB2)
#if NETFRAMEWORK
					|| connectionOptions.ProviderName == DB2iSeriesAccessClientProviderAdapter.AssemblyName
#endif
					)
			{
				if (TryGetDataProvider(connectionOptions.ConfigurationString, out var dataProvider))
					return dataProvider;

				if (AutoDetectProvider)

					return providerDetector.AutoDetectDataProvider(connectionOptions.ConnectionString);
			}

			return null;
		}

		public static IDataProvider ProviderDetector(IConnectionStringSettings css, string connectionString)
		{
			return ProviderDetector(new ConnectionOptions
			{
				ConfigurationString = css.Name,
				ConnectionString = !string.IsNullOrEmpty(css.ConnectionString) ? css.ConnectionString : connectionString,
			});
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
			DbConnection connection, 
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString)
		{
			return new DataConnection(GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString)), connection);
		}

		public static DataConnection CreateDataConnection(
			DbTransaction transaction,
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString)
		{
			return new DataConnection(GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString)), transaction);
		}

		#endregion

		#region BulkCopy

		/// <summary>
		/// Default bulk copy mode, used for DB2i by <see cref="DataConnectionExtensions.BulkCopy{T}(DataConnection, IEnumerable{T})"/>
		/// methods, if mode is not specified explicitly.
		/// Default value: <see cref="BulkCopyType.MultipleRows"/>.
		/// </summary>
		public static BulkCopyType DefaultBulkCopyType = BulkCopyType.MultipleRows;

		#endregion
	}
}
