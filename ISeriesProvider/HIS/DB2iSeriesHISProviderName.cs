using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesHISProviderName
	{
		public const string DB2 = "DB2.iSeriesHIS";

		public const string DB2_GAS = "DB2.iSeriesHIS.GAS";

		public const string DB2_73 = "DB2.iSeriesHIS.73";

		public const string DB2_73_GAS = "DB2.iSeriesHIS.73.GAS";

		public static string[] AllNames = {DB2, DB2_GAS, DB2_73, DB2_73_GAS};
	}
}
