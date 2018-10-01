namespace LinqToDB.DataProvider.DB2iSeries
{
    public static partial class DB2iSeriesTools
    {
        public class DB2iSeriesServerVersion
        {
            public DB2iSeriesServerVersion(int major, int minor, string ptfGroupName)
            {
                Major = major;
                Minor = minor;
                PtfGroupName = ptfGroupName;
            }

            public int Major { get; }
            public int Minor { get; }
            public string PtfGroupName { get; }
        }

        #endregion
    }
}