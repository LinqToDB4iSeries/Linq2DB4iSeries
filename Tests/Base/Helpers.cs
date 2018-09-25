using LinqToDB.Data;
using LinqToDB.DataProvider;
using LinqToDB.DataProvider.DB2iSeries;

namespace Tests.Base
{
    public static class Helpers
    {
		public static string GetDummyFrom(DataConnection dataConnection) 
            => dataConnection.DataProvider is DB2iSeriesDataProvider dataProvider ? 
                $" FROM {DB2iSeriesTools.GetDB2DummyTableName(dataConnection.Connection)}" : string.Empty;
	}
}
