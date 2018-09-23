using LinqToDB.Extensions;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
    public static class DB2iSeriesProviderName
    {
        //Default provider names 
        //This provides compatibility with previous versions 
        //Also provides a way provide shortcuts to "default" options
        [VirtualDataProvider(DB2iSeries_AccessClient)]
        public const string DB2 = "DB2.iSeries";
        [VirtualDataProvider(DB2iSeries_AccessClient_GAS)]
        public const string DB2_GAS = "DB2.iSeries.GAS";
        [VirtualDataProvider(DB2iSeries_AccessClient_73)]
        public const string DB2_73 = "DB2.iSeries.73";
        [VirtualDataProvider(DB2iSeries_AccessClient_73_GAS)]
        public const string DB2_73_GAS = "DB2.iSeries.73.GAS";

        //Provider names per ADO provider type
        [DataProviderOptions(DB2iSeriesLevels.Any, false, DB2iSeriesAdoProviderType.AccessClient)]
        public const string DB2iSeries_AccessClient = "DB2.iSeries.AccessClient";
        [DataProviderOptions(DB2iSeriesLevels.Any, false, DB2iSeriesAdoProviderType.DB2Connect)]
        public const string DB2iSeries_DB2Connect = "DB2.iSeries.DB2Connect";

        //Provider names per ADO provider type for GAS
        [DataProviderOptions(DB2iSeriesLevels.Any, true, DB2iSeriesAdoProviderType.AccessClient)]
        public const string DB2iSeries_AccessClient_GAS = "DB2.iSeries.AccessClient.GAS";
        [DataProviderOptions(DB2iSeriesLevels.Any, false, DB2iSeriesAdoProviderType.DB2Connect)]
        public const string DB2iSeries_DB2Connect_GAS = "DB2.iSeries.DB2Connect.GAS";

        //Provider names including version
        [DataProviderOptions(DB2iSeriesLevels.V7_1_38, false, DB2iSeriesAdoProviderType.AccessClient)]
        public const string DB2iSeries_AccessClient_73 = "DB2.iSeries.AccessClient.73";
        [DataProviderOptions(DB2iSeriesLevels.V7_1_38, false, DB2iSeriesAdoProviderType.DB2Connect)]
        public const string DB2iSeries_DB2Connect_73 = "DB2.iSeries.DB2Connect.73";

        [DataProviderOptions(DB2iSeriesLevels.V7_1_38, true, DB2iSeriesAdoProviderType.AccessClient)]
        public const string DB2iSeries_AccessClient_73_GAS = "DB2.iSeries.AccessClient.73.GAS";
        [DataProviderOptions(DB2iSeriesLevels.V7_1_38, true, DB2iSeriesAdoProviderType.AccessClient)]
        public const string DB2iSeries_DB2Connect_73_GAS = "DB2.iSeries.DB2Connect.73.GAS";

        //Turned to hashset for faster query execution
        //Used reflection to add names automatically
        public static readonly HashSet<string> AllNames
            = new HashSet<string>(
                typeof(DB2iSeriesProviderName)
                    .GetFieldsEx()
                    .Where(x => x.FieldType == typeof(string))
                    .Select(x => x.GetValue(null).ToString()));

        private static readonly IReadOnlyDictionary<string, string> actualNames
            = typeof(DB2iSeriesProviderName)
                    .GetFieldsEx()
                    .Where(x => x.FieldType == typeof(string))
                    .Select(x => new
                    {
                        Name = x.GetValue(null).ToString(),
                        ActualName = x.GetCustomAttributesEx(false).OfType<VirtualDataProviderAttribute>().FirstOrDefault()
                    })
                    .Where(x => x.ActualName != null)
                    .ToDictionary(x => x.Name, x => x.ActualName.ActualProviderName);

        private static readonly IReadOnlyDictionary<string, DB2iSeriesDataProviderOptions> options
            = typeof(DB2iSeriesProviderName)
                    .GetFieldsEx()
                    .Where(x => x.FieldType == typeof(string))
                    .Select(x => new
                    {
                        Name = x.GetValue(null).ToString(),
                        Options = x.GetCustomAttributesEx(false).OfType<DataProviderOptionsAttribute>().FirstOrDefault()
                    })
                    .Where(x => x.Options != null)
                    .ToDictionary(x => x.Name, x => x.Options.Options);

        private static readonly IReadOnlyDictionary<DB2iSeriesDataProviderOptions, string> fromOptions =
            options.ToDictionary(x => x.Value, x => x.Key);

        public static string GetFromOptions(DB2iSeriesDataProviderOptions options) => fromOptions[options];
        public static DB2iSeriesDataProviderOptions GetOptions(string providerName) => options[providerName];

        public static string GetActualProviderName(string providerName)
        {
            return actualNames[providerName];
        }

        public static bool IsVirtual(string providerName)
        {
            return providerName != actualNames[providerName];
        }
    }
}
