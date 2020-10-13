using LinqToDB.Common;
using LinqToDB.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesProviderDetector
	{
		public struct DB2iSeriesServerVersion
		{
			public DB2iSeriesServerVersion(int major, int minor, int patchLevel)
			{
				Major = major;
				Minor = minor;
				PatchLevel = patchLevel;
			}

			public int Major { get; }
			public int Minor { get; }
			public int PatchLevel { get; }
		}

		public IDB2iSeriesDataProvider AutoDetectDataProvider(string connectionString)
		{
			try
			{
				var providerType = GetProviderType(connectionString);
				var minLevel = GetServerMinLevel(connectionString, providerType);


				var providerName = providerType switch
				{
					DB2iSeriesAdoProviderType.AccessClient => minLevel switch
					{
						DB2iSeriesLevels.V7_1_38 => DB2iSeriesProviderName.DB2_73,
						_ => DB2iSeriesProviderName.DB2,
					},
					DB2iSeriesAdoProviderType.OleDb => minLevel switch
					{
						DB2iSeriesLevels.V7_1_38 => DB2iSeriesProviderName.DB2_OleDb,
						_ => DB2iSeriesProviderName.DB2_OleDb,
					},
					DB2iSeriesAdoProviderType.DB2 => minLevel switch
					{
						DB2iSeriesLevels.V7_1_38 => DB2iSeriesProviderName.DB2_DB2Connect,
						_ => DB2iSeriesProviderName.DB2_DB2Connect,
					},
					DB2iSeriesAdoProviderType.Odbc => minLevel switch
					{
						DB2iSeriesLevels.V7_1_38 => DB2iSeriesProviderName.DB2_ODBC,
						_ => DB2iSeriesProviderName.DB2_ODBC,
					},
					_ => throw new LinqToDBException($"Invalid i series provider type {providerType}")
				};

				return DB2iSeriesTools.GetDataProvider(providerName);
			}
			catch (Exception e)
			{
				throw new LinqToDBException($"Failed to detect DB2iSeries provider from given string: {connectionString}. Error: {e.Message}");
			}
		}

		private DB2iSeriesAdoProviderType GetProviderType(string connectionString)
		{
			var csb = new DbConnectionStringBuilder()
			{
				ConnectionString = connectionString.ToUpper()
			};

			if (csb.ContainsKey("DRIVER"))
				return DB2iSeriesAdoProviderType.Odbc;
			else if (csb.ContainsKey("PROVIDER"))
				return DB2iSeriesAdoProviderType.OleDb;
			else if (csb.ContainsKey("SERVER"))
				return DB2iSeriesAdoProviderType.DB2;
			else if (csb.ContainsKey("DATA SOURCE"))
				return DB2iSeriesAdoProviderType.AccessClient;
			else
				throw new LinqToDBException("Connection string doesn't seem to be a valid DB2iSeries connection string.");
		}

		private DB2iSeriesServerVersion GetServerVersion(string connectionString, DB2iSeriesAdoProviderType providerType)
		{
			using (var conn = (DbConnection)DB2iSeriesTools.GetDataProvider(providerType).CreateConnection(connectionString))
			{
				conn.Open();

				var serverVersionParts = conn.ServerVersion.Substring(0, 5).Split('.');
				var major = int.Parse(serverVersionParts.First());
				var minor = int.Parse(serverVersionParts.Last());

				var patchLevel = GetMaxPatchLevel(conn, major, minor);

				return new DB2iSeriesServerVersion(major, minor, patchLevel);
			}
		}

		private DB2iSeriesLevels GetServerMinLevel(string connectionString, DB2iSeriesAdoProviderType providerType)
		{
			var version = GetServerVersion(connectionString, providerType);

			if (version.Major > 7 || version.Minor > 2)
				return DB2iSeriesLevels.V7_1_38;

			if (version.Major == 7
				&& (version.Minor == 1 || version.Minor == 2))
			{
				if ((version.Minor == 1 && version.PatchLevel > 38)
					|| (version.Minor == 2 && version.PatchLevel > 9))
					return DB2iSeriesLevels.V7_1_38;
			}

			return DB2iSeriesLevels.Any;
		}

		private int GetMaxPatchLevel(DbConnection connection, int major, int minor)
		{
			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText =
					$"SELECT MAX(PTF_GROUP_LEVEL) FROM QSYS2.GROUP_PTF_INFO WHERE PTF_GROUP_NAME = 'SF99{major:D1}{minor:D2}' AND PTF_GROUP_STATUS = 'INSTALLED'";
				//var param = cmd.CreateParameter();
				//param.ParameterName = "p1";
				//param.Value = $"SF99{major:D1}{minor:D2}";

				//cmd.Parameters.Add(param);

				return Converter.ChangeTypeTo<int>(cmd.ExecuteScalar());
			}
		}
	}
}
