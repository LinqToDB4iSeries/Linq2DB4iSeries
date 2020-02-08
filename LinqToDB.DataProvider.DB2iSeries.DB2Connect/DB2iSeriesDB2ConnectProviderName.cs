using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesDB2ConnectProviderName
	{
		public const string DB2 = "DB2.iSeriesDB2Connect";

		public const string DB2_GAS = "DB2.iSeriesDB2Connect.GAS";

		public const string DB2_73 = "DB2.iSeriesDB2Connect.73";

		public const string DB2_73_GAS = "DB2.iSeriesDB2Connect.73.GAS";

		public static string[] AllNames = {DB2, DB2_GAS, DB2_73, DB2_73_GAS};
	}
}
