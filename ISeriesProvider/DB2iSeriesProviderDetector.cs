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
		private readonly bool mapGuidAsString;

		public DB2iSeriesProviderDetector(bool mapGuidAsString = false)
		{
			this.mapGuidAsString = mapGuidAsString;
		}

		public DB2iSeriesDataProvider AutoDetectDataProvider(string connectionString)
		{
			try
			{
				var providerType = GetProviderType(connectionString);
				var minLevel = GetServerMinLevel(connectionString, providerType);

				return DB2iSeriesTools.GetDataProvider(minLevel, providerType, new DB2iSeriesMappingOptions(mapGuidAsString));
			}
			catch (Exception e)
			{
				throw ExceptionHelper.ConnectionStringParsingFailure(e);
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
				throw ExceptionHelper.InvalidConnectionString();
		}

		private ServerVersion GetServerVersion(string connectionString, DB2iSeriesAdoProviderType providerType)
		{
			using (var conn = (DbConnection)DB2iSeriesTools.GetDataProvider(DB2iSeriesVersion.V7_1, providerType, DB2iSeriesMappingOptions.Default).CreateConnection(connectionString))
			{
				conn.Open();

				var serverVersionParts = conn.ServerVersion.Substring(0, 5).Split('.');
				var major = int.Parse(serverVersionParts.First());
				var minor = int.Parse(serverVersionParts.Last());

				var patchLevel = GetMaxPatchLevel(conn, major, minor);

				return new ServerVersion(major, minor, patchLevel);
			}
		}

		private DB2iSeriesVersion GetServerMinLevel(string connectionString, DB2iSeriesAdoProviderType providerType)
		{
			var version = GetServerVersion(connectionString, providerType);
			
			return version switch
			{
				var x when x >= new ServerVersion(7, 2, 9) => DB2iSeriesVersion.V7_3,
				var x when x >= new ServerVersion(7, 2, 0) => DB2iSeriesVersion.V7_2,
				_ => DB2iSeriesVersion.V7_1
			};
		}

		private int GetMaxPatchLevel(DbConnection connection, int major, int minor)
		{
			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText =
					$"SELECT MAX(PTF_GROUP_LEVEL) FROM QSYS2.GROUP_PTF_INFO WHERE PTF_GROUP_NAME = 'SF99{major:D1}{minor:D2}' AND PTF_GROUP_STATUS = 'INSTALLED'";
				
				return Converter.ChangeTypeTo<int>(cmd.ExecuteScalar());
			}
		}
	}
}
