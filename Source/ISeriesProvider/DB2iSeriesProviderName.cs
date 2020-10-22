using LinqToDB.Extensions;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesProviderName
	{
		public const string DB2 = "DB2.iSeries";
		public const string DB2_GAS = "DB2.iSeries.GAS";


		//public const string DB2_73 = "DB2.iSeries.73";

		//public const string DB2_73_GAS = "DB2.iSeries.73.GAS";

		public const string DB2_ODBC_71 = "DB2.iSeries.ODBC.71";
		public const string DB2_ODBC_72 = "DB2.iSeries.ODBC.72";
		public const string DB2_ODBC_73 = "DB2.iSeries.ODBC.73";
		public const string DB2_ODBC_71_GAS = "DB2.iSeries.ODBC.71.GAS";
		public const string DB2_ODBC_72_GAS = "DB2.iSeries.ODBC.72.GAS";
		public const string DB2_ODBC_73_GAS = "DB2.iSeries.ODBC.73.GAS";

		public const string DB2_OleDb_71 = "DB2.iSeries.OleDb.71";
		public const string DB2_OleDb_72 = "DB2.iSeries.OleDb.72";
		public const string DB2_OleDb_73 = "DB2.iSeries.OleDb.73";
		public const string DB2_OleDb_71_GAS = "DB2.iSeries.OleDb.71.GAS";
		public const string DB2_OleDb_72_GAS = "DB2.iSeries.OleDb.72.GAS";
		public const string DB2_OleDb_73_GAS = "DB2.iSeries.OleDb.73.GAS";

		public const string DB2_AccessClient_71 = "DB2.iSeries.Net.71";
		public const string DB2_AccessClient_72 = "DB2.iSeries.Net.72";
		public const string DB2_AccessClient_73 = "DB2.iSeries.Net.73";
		public const string DB2_AccessClient_71_GAS = "DB2.iSeries.Net.71.GAS";
		public const string DB2_AccessClient_72_GAS = "DB2.iSeries.Net.72.GAS";
		public const string DB2_AccessClient_73_GAS = "DB2.iSeries.Net.73.GAS";

		public const string DB2_DB2Connect_71 = "DB2.iSeries.DB2Connect.71";
		public const string DB2_DB2Connect_72 = "DB2.iSeries.DB2Connect.72";
		public const string DB2_DB2Connect_73 = "DB2.iSeries.DB2Connect.73";
		public const string DB2_DB2Connect_71_GAS = "DB2.iSeries.DB2Connect.71.GAS";
		public const string DB2_DB2Connect_72_GAS = "DB2.iSeries.DB2Connect.72.GAS";
		public const string DB2_DB2Connect_73_GAS = "DB2.iSeries.DB2Connect.73.GAS";

		public static string[] AllNames =
			typeof(DB2iSeriesProviderName)
			.GetFields()
			.Where(x => x.GetMemberType() == typeof(string))
			.Select(x => x.GetValue(null) as string)
			.Distinct()
			.ToArray();

		public static DB2iSeriesProviderType GetProviderType(string providerName)
		{
			if (providerName.Contains(".OleDb"))
				return DB2iSeriesProviderType.OleDb;
			else if (providerName.Contains(".ODBC"))
				return DB2iSeriesProviderType.Odbc;
#if NETFRAMEWORK
			else if (providerName.Contains(".Net"))
				return DB2iSeriesProviderType.AccessClient;
#endif
			else if (providerName.Contains(".DB2Connect"))
				return DB2iSeriesProviderType.DB2;
			else
				throw ExceptionHelper.InvalidProviderName(providerName);
		}

		public static DB2iSeriesProviderOptions GetProviderOptions(string providerName)
		{
			var providerType = GetProviderType(providerName);
			
			var version = DB2iSeriesVersion.V7_1;
			
			if (providerName.Contains("72"))
				version = DB2iSeriesVersion.V7_2;
			else if (providerName.Contains("73"))
				version = DB2iSeriesVersion.V7_3;

			var mapGuidAsString = providerName.Contains("GAS");

			return new DB2iSeriesProviderOptions(providerName, providerType, version)
			{
				MapGuidAsString = mapGuidAsString
			};
		}

		public static string GetProviderName(
			DB2iSeriesVersion version,
			DB2iSeriesProviderType providerType,
			DB2iSeriesMappingOptions mappingOptions)
		{
			var mapGuidAsString = mappingOptions.MapGuidAsString;

			return providerType switch
			{
#if NETFRAMEWORK
				DB2iSeriesProviderType.AccessClient => version switch
				{
					DB2iSeriesVersion.V7_1 => mapGuidAsString ? DB2_AccessClient_71_GAS : DB2_AccessClient_71,
					DB2iSeriesVersion.V7_2 => mapGuidAsString ? DB2_AccessClient_72_GAS : DB2_AccessClient_72,
					_ => mapGuidAsString ? DB2_AccessClient_73_GAS : DB2_AccessClient_73,
				},
#endif
				DB2iSeriesProviderType.Odbc => version switch
				{
					DB2iSeriesVersion.V7_1 => mapGuidAsString ? DB2_ODBC_71_GAS : DB2_ODBC_71,
					DB2iSeriesVersion.V7_2 => mapGuidAsString ? DB2_ODBC_72_GAS : DB2_ODBC_72,
					_ => mapGuidAsString ? DB2_ODBC_73_GAS : DB2_ODBC_73,
				},
				DB2iSeriesProviderType.OleDb => version switch
				{
					DB2iSeriesVersion.V7_1 => mapGuidAsString ? DB2_OleDb_71_GAS : DB2_OleDb_71,
					DB2iSeriesVersion.V7_2 => mapGuidAsString ? DB2_OleDb_72_GAS : DB2_OleDb_72,
					_ => mapGuidAsString ? DB2_OleDb_73_GAS : DB2_OleDb_73,
				},
				DB2iSeriesProviderType.DB2 => version switch
				{
					DB2iSeriesVersion.V7_1 => mapGuidAsString ? DB2_DB2Connect_71_GAS : DB2_DB2Connect_71,
					DB2iSeriesVersion.V7_2 => mapGuidAsString ? DB2_DB2Connect_72_GAS : DB2_DB2Connect_72,
					_ => mapGuidAsString ? DB2_DB2Connect_73_GAS : DB2_DB2Connect_73,
				},
				_ => throw ExceptionHelper.InvalidAdoProvider(providerType)
			};
		}
	}
}
