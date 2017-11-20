using LinqToDB.SqlProvider;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
    public class DB2iSeriesSqlProviderFlags
    {
        public bool SupportsLimitAndOffset { get; set; }

        public static DB2iSeriesSqlProviderFlags Defaults = new DB2iSeriesSqlProviderFlags();

        public DB2iSeriesSqlProviderFlags Clone()
        {
            return this.MemberwiseClone() as DB2iSeriesSqlProviderFlags;
        }
    }
}
