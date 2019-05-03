﻿using System;
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
          ITable<T> table,
          BulkCopyOptions options,
          IEnumerable<T> source)
        {
            throw new NotImplementedException("Not able to do bulk copy in DB2iSeries Provider.");
        }

        protected override BulkCopyRowsCopied MultipleRowsCopy2<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, string from)
        {
            if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_BATCH_SIZE)
            {
                options.MaxBatchSize = MAX_ALLOWABLE_BATCH_SIZE;
            }

            return base.MultipleRowsCopy2(new DB2iSeriesMultipleRowsHelper<T>(table, options), source, from);
        }

        protected override BulkCopyRowsCopied MultipleRowsCopy<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source)
        {
            if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_BATCH_SIZE)
            {
                options.MaxBatchSize = MAX_ALLOWABLE_BATCH_SIZE;
            }

            return MultipleRowsCopy2(table, options, source, " FROM " + DB2iSeriesTools.iSeriesDummyTableName());
        }
    }
}