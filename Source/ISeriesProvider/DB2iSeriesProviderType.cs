namespace LinqToDB.DataProvider.DB2iSeries
{
	public enum DB2iSeriesProviderType
	{
		Odbc = 0,
		OleDb = 1,
		DB2 = 2,
#if NETFRAMEWORK
		AccessClient = 3
#endif
	}
}
