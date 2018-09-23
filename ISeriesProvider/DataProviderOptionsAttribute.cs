using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
    //Attribute used to bind options to a provide name
    internal class DataProviderOptionsAttribute : Attribute
    {
        public DataProviderOptionsAttribute(DB2iSeriesLevels minLevel, bool mapGuidAsString, DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            Options = new DB2iSeriesDataProviderOptions(minLevel, mapGuidAsString, dB2AdoProviderType);
        }

        public DB2iSeriesDataProviderOptions Options { get; }
    }
}