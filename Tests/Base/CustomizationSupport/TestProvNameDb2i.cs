using System;
using System.Collections.Generic;
using System.Linq;
using LinqToDB.DataProvider.DB2iSeries;

namespace Tests
{
	public static class TestProvNameDb2i
	{
		public const string DB2iBase = "DB2.iSeries";

		public const string All = DB2iBase;
		public const string All_AccessClient = "DB2.iSeries.Net";
		public const string All_ODBC = "DB2.iSeries.ODBC";
		public const string All_OleDb = "DB2.iSeries.OleDb";
		public const string All_DB2Connect = "DB2.iSeries.DB2Connect";

		public const string All_71 = "DB2.iSeries.71";
		public const string All_72 = "DB2.iSeries.72";
		public const string All_73 = "DB2.iSeries.73";
		public const string All_74 = "DB2.iSeries.74";

		public const string All_NonGAS = "DB2.iSeries.NonGAS";
		public const string All_GAS = "DB2.iSeries.GAS";
		
		private static IEnumerable<string> GetProviders(Func<DB2iSeriesProviderOptions, bool> predicate)
			=> DB2iSeriesProviderName.AllNames.Where(x =>
			predicate(DB2iSeriesProviderName.GetProviderOptions(x)));

		private static IEnumerable<string> GetProviders(Func<string, bool> predicate)
			=> DB2iSeriesProviderName.AllNames.Where(predicate);

		private static IEnumerable<string> GetProvidersContaining(string contains)
			=> DB2iSeriesProviderName.AllNames.Where(x => x.Contains(contains));

		public static IEnumerable<string> GetAll() => GetProviders((string _) => true);

		public static IEnumerable<string> GetAccessClient() =>
#if NET472
			GetProviders(x => x.ProviderType == DB2iSeriesProviderType.AccessClient);
#else
			Enumerable.Empty<string>();
#endif
		public static IEnumerable<string> GetODBC() => GetProviders(x => x.ProviderType == DB2iSeriesProviderType.Odbc);
		public static IEnumerable<string> GetOleDb() => GetProviders(x => x.ProviderType == DB2iSeriesProviderType.OleDb);
		public static IEnumerable<string> GetDB2Connect() => GetProviders(x => x.ProviderType == DB2iSeriesProviderType.DB2);

		public static IEnumerable<string> Get71() => GetProvidersContaining("71");
		public static IEnumerable<string> Get72() => GetProvidersContaining("72");
		public static IEnumerable<string> Get73() => GetProvidersContaining("73");
		public static IEnumerable<string> Get74() => GetProvidersContaining("74");

		public static IEnumerable<string> GetGAS() => GetProvidersContaining("GAS");
		public static IEnumerable<string> GetNonGAS() => GetProviders(x => !x.Contains("GAS"));

		public static IEnumerable<string> GetProviders(string context)
			=> context switch
			{
				LinqToDB.ProviderName.DB2 => GetAll(),
				All => GetAll(),
				All_AccessClient => GetAccessClient(),
				All_ODBC => GetODBC(),
				All_OleDb => GetOleDb(),
				All_DB2Connect => GetDB2Connect(),

				All_71 => Get71(),
				All_72 => Get72(),
				All_73 => Get73(),
				All_74 => Get74(),

				All_GAS => GetGAS(),
				All_NonGAS => GetNonGAS(),
				_ => Enumerable.Empty<string>()
			};


		public static IEnumerable<string> GetProviders(IEnumerable<string> contexts)
			=> contexts.SelectMany(GetProviders).Distinct();
		
		public static string GetConcatenatedProviders(IEnumerable<string> contexts)
			=> string.Join(",", GetProviders(contexts));

		public static bool IsiSeries(string provider) => provider.StartsWith(DB2iBase);
		public static bool IsiSeriesODBC(string provider) => provider.StartsWith(DB2iBase) && provider.ToUpper().Contains("ODBC");
		public static bool IsiSeriesOleDb(string provider) => provider.StartsWith(DB2iBase) && provider.ToUpper().Contains("OLEDB");
		public static bool IsiSeriesDB2Connect(string provider) => provider.StartsWith(DB2iBase) && provider.ToUpper().Contains("CONNECT");
		public static bool IsiSeriesAccessClient(string provider) => IsiSeries(provider) && !IsiSeriesODBC(provider) && !IsiSeriesOleDb(provider) && !IsiSeriesDB2Connect(provider);

		public static string GetFamily(string provider)
		{
			if (IsiSeries(provider))
				return DB2iBase;
			else 
				return provider;
		}
	}
}
