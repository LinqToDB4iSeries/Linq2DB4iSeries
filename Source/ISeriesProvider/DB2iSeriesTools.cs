using LinqToDB.Data;
using System;
using System.Collections.Concurrent;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesTools
	{
		#region DataProvider instances

		private static readonly ConcurrentDictionary<string, DB2iSeriesDataProvider> dataProviders = new();

		/// <summary>
		/// Detects and returns an appropriate data provider for DB2 iSeries based on the specified connection options.
		/// </summary>
		/// <remarks>If multiple connection options are provided, the method prioritizes explicit <see
		/// cref="DbConnection"/> and <see cref="DbTransaction"/> objects over the connection string. This method is typically
		/// used to obtain a provider for executing database operations against DB2 iSeries.</remarks>
		/// <param name="connectionString">The connection string used to establish a connection to the DB2 iSeries database. Can be null if a connection
		/// object is provided.</param>
		/// <param name="connection">An existing <see cref="DbConnection"/> instance to use for provider detection. Can be null if a connection string
		/// is provided.</param>
		/// <param name="transaction">An optional <see cref="DbTransaction"/> associated with the connection. Used to detect the provider in
		/// transactional contexts.</param>
		/// <returns>An <see cref="IDataProvider"/> instance that matches the specified connection options for DB2 iSeries.</returns>
		/// <exception cref="LinqToDBException">Thrown if a suitable data provider cannot be detected from the provided connection options.</exception>
		public static IDataProvider GetDataProvider(string? connectionString = null, DbConnection? connection = null, DbTransaction? transaction = null)
		{
			return providerDetector.DetectProvider(new ConnectionOptions(ConnectionString: connectionString, DbConnection: connection, DbTransaction: transaction, ProviderName: DB2iSeriesProviderName.DB2))
				?? throw ExceptionHelper.CouldNotDetectProvider();

		}

		internal static DB2iSeriesDataProvider GetDataProvider(string providerName)
		{
			return dataProviders.GetOrAdd(providerName,
				p => new DB2iSeriesDataProvider(DB2iSeriesProviderName.GetProviderOptions(p)));
		}

		/// <summary>
		/// Returns a configured DB2iSeries data provider instance for the specified database version, provider type, and
		/// mapping options.
		/// </summary>
		/// <param name="version">The DB2 for iSeries database version to target. Determines compatibility and supported features.</param>
		/// <param name="providerType">The type of provider to use for database connectivity. Specifies the underlying driver or access method.</param>
		/// <param name="mappingOptions">The mapping options to apply when translating between .NET types and DB2iSeries types. Controls data conversion
		/// and schema mapping behavior.</param>
		/// <returns>A DB2iSeriesDataProvider instance configured according to the specified version, provider type, and mapping
		/// options.</returns>
		public static DB2iSeriesDataProvider GetDataProvider(
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			DB2iSeriesMappingOptions mappingOptions)
		{
			return GetDataProvider(DB2iSeriesProviderName.GetProviderName(version, providerType, mappingOptions));
		}

		#endregion

		#region AutoDetection

		private readonly static DB2iSeriesProviderDetector providerDetector = new();

		public static bool AutoDetectProvider
		{
			get => providerDetector.AutoDetectProvider;
			set => providerDetector.AutoDetectProvider = value;
		}

		public static void RegisterProviderDetector()
		{
			DataConnection.AddProviderDetector(providerDetector.DetectProvider);
		}

		#endregion

		#region ConnectionString

		/// <summary>
		/// Applies provider-specific sanitization to the supplied connection string builder and returns the resulting
		/// connection string.
		/// </summary>
		/// <remarks>This method modifies the provided <paramref name="connectionString"/> in place by setting or updating
		/// provider-specific connection string properties. The returned connection string reflects these changes. If the
		/// builder's provider type does not match the specified <paramref name="providerType"/>, an exception is
		/// thrown.</remarks>
		/// <param name="connectionString">The connection string containing the initial connection parameters to be sanitized. Cannot be null.</param>
		/// <param name="providerType">The DB2 iSeries provider type that determines which sanitization rules are applied to the connection string. Auto-detected if null.</param>
		/// <returns></returns>
		/// <exception cref="ArgumentNullException">Thrown if <paramref name="connectionString"/> is null.</exception>
		/// <exception cref="LinqToDBException">Thrown if the provider type cannot be determined from the connection string, or if the specified provider type is invalid.</exception>
		public static string SanitizeConnectionString(string connectionString, DB2iSeriesProviderType? providerType = null)
		{
			if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

			var connectionStringBuilder = new DbConnectionStringBuilder(true) { ConnectionString = connectionString };

			if (providerType == null)
			{
				var csProviderType = DB2iSeriesProviderDetector.GetProviderType(connectionStringBuilder)
					?? throw ExceptionHelper.InvalidConnectionString();

				providerType = csProviderType;
			}
			
			switch (providerType)
			{
				case DB2iSeriesProviderType.Odbc:
					connectionStringBuilder["NAM"] = 0;
					connectionStringBuilder["UNICODESQL"] = 1;
					connectionStringBuilder["MAXDECPREC"] = 63;
					connectionStringBuilder["MAXDECSCALE"] = 63;
					connectionStringBuilder["GRAPHIC"] = 1;
					connectionStringBuilder["MAPDECIMALFLOATDESCRIBE"] = 3;
					connectionStringBuilder["MAXFIELDLEN"] = 2097152;
					connectionStringBuilder["ALLOWUNSCHAR"] = 1;
					break;
				case DB2iSeriesProviderType.OleDb:
					connectionStringBuilder["Naming Convention"] = 1;
					connectionStringBuilder["Maximum Decimal Precision"] = 63;
					connectionStringBuilder["Maximum Decimal Scale"] = 63;
					connectionStringBuilder["Convert Date Time To Char"] = "TRUE";
					connectionStringBuilder["Keep Trailing Blanks"] = "TRUE";
					break;
				case DB2iSeriesProviderType.DB2:
					connectionStringBuilder["Graphic"] = 1;
					break;
#if NETFRAMEWORK
				case DB2iSeriesProviderType.AccessClient:
					connectionStringBuilder["Naming"] = 0;
					break;
#endif
				default:
					throw ExceptionHelper.InvalidAdoProvider(providerType.Value);
			}

			return connectionStringBuilder.ConnectionString;
		}

		#endregion

		#region CreateDataConnection

		public static DataConnection CreateDataConnection(
			string connectionString,
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString,
			bool sanitizeConnectionString = false)
		{
			if (sanitizeConnectionString)
				connectionString = SanitizeConnectionString(connectionString, providerType);

			return new DataConnection(new DataOptions()
				.UseDataProvider(GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString)))
				.UseConnectionString(connectionString));
		}

		public static DataConnection CreateDataConnection(
			DbConnection connection,
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString)
		{
			return new DataConnection(new DataOptions()
				.UseDataProvider(GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString)))
				.UseConnection(connection));
		}

		public static DataConnection CreateDataConnection(
			DbTransaction transaction,
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			bool mapGuidAsString)
		{
			var provider = GetDataProvider(version, providerType, new DB2iSeriesMappingOptions(mapGuidAsString));
			return new DataConnection(new DataOptions()
				.UseDataProvider(provider)
				.UseTransaction(provider, transaction));
		}

		#endregion
	}
}
