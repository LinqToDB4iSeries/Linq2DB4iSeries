namespace LinqToDB.DataProvider.DB2iSeries
{
	public enum DB2iSeriesProviderType
	{
		Odbc,
		OleDb,
		DB2,
#if NETFRAMEWORK
		AccessClient
#endif
	}
}
