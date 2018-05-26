using LinqToDB.DataProvider;
using LinqToDB.DataProvider.DB2iSeries;

namespace Tests.Base
{
    public class Helpers
    {
		public static string GetDummyFrom(IDataProvider provider) => provider is DB2iSeriesDataProvider dataProvider ? $" FROM {dataProvider.DummyTableName}" : string.Empty;
	}
}
