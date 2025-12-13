using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.Internal.DataProvider;
using System;
using System.Data.Common;
using System.Globalization;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class DB2iSeriesProviderDetector : ProviderDetectorBase<DB2iSeriesProviderType, DB2iSeriesVersion>
	{
		private readonly bool mapGuidAsString;

		public DB2iSeriesProviderDetector(bool mapGuidAsString = false)
		{
			this.mapGuidAsString = mapGuidAsString;
		}

		public static DB2iSeriesProviderType? GetProviderType(string connectionString)
		{
			if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
			return GetProviderType(new DbConnectionStringBuilder(true) { ConnectionString = connectionString });
		}

		public static DB2iSeriesProviderType? GetProviderType(DbConnectionStringBuilder dbConnectionStringBuilder)
		{
			if (dbConnectionStringBuilder == null) throw new ArgumentNullException(nameof(dbConnectionStringBuilder));

			if (dbConnectionStringBuilder.ContainsKey("DRIVER"))
				return DB2iSeriesProviderType.Odbc;
			else if (dbConnectionStringBuilder.ContainsKey("PROVIDER"))
				return DB2iSeriesProviderType.OleDb;
			else if (dbConnectionStringBuilder.ContainsKey("SERVER"))
				return DB2iSeriesProviderType.DB2;
#if NETFRAMEWORK
			else if (dbConnectionStringBuilder.ContainsKey("DATA SOURCE"))
				return DB2iSeriesProviderType.AccessClient;
#endif
			else
				return null;
		}

		private static int GetMaxPatchLevel(DbConnection connection, int major, int minor)
		{
			using var cmd = connection.CreateCommand();

			cmd.CommandText =
				$"SELECT MAX(PTF_GROUP_LEVEL) FROM QSYS2.GROUP_PTF_INFO WHERE PTF_GROUP_NAME = 'SF99{major:D1}{minor:D2}' AND PTF_GROUP_STATUS = 'INSTALLED'";

			return Converter.ChangeTypeTo<int>(cmd.ExecuteScalar());
		}

		public override IDataProvider? DetectProvider(ConnectionOptions options)
		{
			// If both ProviderName and ConfigurationString are empty exit detector
			if (string.IsNullOrEmpty(options.ConfigurationString) && string.IsNullOrEmpty(options.ProviderName))
				return null;

			var providerHint = options.ProviderName ?? options.ConfigurationString;

			// If ConfiguratioString is one of the supported names, or the ProviderName matches the known names start detection
			if (DB2iSeriesProviderName.AllNames.Contains(options.ProviderName ?? "")
				|| DB2iSeriesProviderName.AllNames.Contains(options.ConfigurationString ?? "")
				|| options.ProviderName?.StartsWith(DB2iSeriesProviderName.DB2) == true
#if NETFRAMEWORK
				|| options.ProviderName == DB2iSeriesAccessClientProviderAdapter.AssemblyName
#endif
				)
			{
				// Set name to look for
				var name = options.ProviderName ?? options.ConfigurationString ?? DB2iSeriesProviderName.DB2;

				if (DB2iSeriesProviderName.AllNames.Contains(name))
					return DB2iSeriesTools.GetDataProvider(name);

				// If no provider was found by name try to auto detect from connection string
				if (AutoDetectProvider && options.ConnectionString is not null)
				{
					var providerType = GetProviderType(options.ConnectionString);

					if (providerType.HasValue)
					{
						var version = DetectServerVersion(options, providerType.Value) ?? DB2iSeriesProviderOptions.Defaults.Version;

						return GetDataProvider(options, providerType.Value, version);
					}
				}
			}

			return null;
		}

		public override IDataProvider GetDataProvider(ConnectionOptions options, DB2iSeriesProviderType provider, DB2iSeriesVersion version)
		{
			var providerIsValid = Enum.IsDefined(typeof(DB2iSeriesProviderType), provider);
			var versionIsValid = Enum.IsDefined(typeof(DB2iSeriesVersion), version);
			
			if (providerIsValid)
			{
				if (!versionIsValid)
					version = DetectServerVersion(options, provider) ?? DB2iSeriesProviderOptions.Defaults.Version;
				
				return DB2iSeriesTools.GetDataProvider(version, provider, new DB2iSeriesMappingOptions(mapGuidAsString));
			}
			else
			{
				var providerImpl = DetectProvider(options);
				if (providerImpl != null)
					return providerImpl;

				return DB2iSeriesTools.GetDataProvider(DB2iSeriesProviderName.DB2);
			}
		}

		protected override DB2iSeriesVersion? DetectServerVersion(DbConnection connection, DbTransaction? _)
		{
			var serverVersionParts = connection.ServerVersion.Substring(0, 5).Split('.');
			var major = int.Parse(serverVersionParts.First(), CultureInfo.InvariantCulture);
			var minor = int.Parse(serverVersionParts.Last(), CultureInfo.InvariantCulture);
			var patchLevel = 0;

			try
			{
				patchLevel = GetMaxPatchLevel(connection, major, minor);
			}
			catch
			{
				return null;
			}

			return new ServerVersion(major, minor, patchLevel) switch
			{
				var x when x > new ServerVersion(7, 3, 11) => DB2iSeriesVersion.V7_4,
				var x when x > new ServerVersion(7, 2, 9) => DB2iSeriesVersion.V7_3,
				var x when x >= new ServerVersion(7, 2, 0) => DB2iSeriesVersion.V7_2,
				_ => DB2iSeriesVersion.V7_1
			};
		}

		protected override DbConnection CreateConnection(DB2iSeriesProviderType provider, string connectionString)
		{
			return DB2iSeriesProviderAdapter.GetInstance(provider).CreateConnection(connectionString);
		}
	}
}
