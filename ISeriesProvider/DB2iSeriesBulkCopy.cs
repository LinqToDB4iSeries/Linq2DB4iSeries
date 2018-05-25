using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using Data;
    using SqlProvider;

    class DB2iSeriesBulkCopy : BasicBulkCopy
    {
        const int MAX_ALLOWABLE_BATCH_SIZE = 100;

        protected override BulkCopyRowsCopied ProviderSpecificCopy<T>(
          DataConnection dataConnection,
          BulkCopyOptions options,
          IEnumerable<T> source)
        {
            throw new NotImplementedException("Not able to do bulk copy in DB2iSeries Provider.");
        }

        protected override BulkCopyRowsCopied MultipleRowsCopy2<T>(DataConnection dataConnection, BulkCopyOptions options, IEnumerable<T> source, string from)
        {
            if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_BATCH_SIZE)
            {
                options.MaxBatchSize = MAX_ALLOWABLE_BATCH_SIZE;
            }

            return MultipleRowsCopy2<T>(
                new DB2iSeriesMultipleRowsHelper<T>(dataConnection, options),
                dataConnection,
                options,
                source,
                from);
        }

        protected override BulkCopyRowsCopied MultipleRowsCopy<T>(DataConnection dataConnection, BulkCopyOptions options, IEnumerable<T> source)
        {
            if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_BATCH_SIZE)
            {
                options.MaxBatchSize = MAX_ALLOWABLE_BATCH_SIZE;
            }

            return MultipleRowsCopy2(dataConnection, options, source, " FROM " + DB2iSeriesTools.iSeriesDummyTableName());
        }
    }
}