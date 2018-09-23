using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
    //This attribute is used to map virtual provider names to actual provider names
    public class VirtualDataProviderAttribute : Attribute
    {
        public VirtualDataProviderAttribute(string actualProviderName)
        {
            ActualProviderName = actualProviderName;
        }

        public string ActualProviderName { get; set; }
    }
}
