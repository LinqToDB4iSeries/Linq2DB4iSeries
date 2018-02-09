using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public static class DB2iSeriesProviderName
	{
		public const string DB2 = "DB2.iSeries";

		public const string DB2_GAS = "DB2.iSeries.GAS";

		public const string DB2_73 = "DB2.iSeries.73";

		public const string DB2_73_GAS = "DB2.iSeries.73.GAS";

		public static string[] AllNames = {DB2, DB2_73, DB2_73_GAS, DB2_GAS};
	}
}
