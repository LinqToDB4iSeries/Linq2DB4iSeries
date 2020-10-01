using System;
using System.Collections.Generic;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using System.Threading;
	using System.Threading.Tasks;

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

		protected override BulkCopyRowsCopied MultipleRowsCopy<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source)
		{
			if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_BATCH_SIZE)
			{
				options.MaxBatchSize = MAX_ALLOWABLE_BATCH_SIZE;
			}

			return MultipleRowsCopy2(new DB2iSeriesMultipleRowsHelper<T>(table, options), source, " FROM " + Constants.SQL.DummyTableName());
		}

		protected override Task<BulkCopyRowsCopied> MultipleRowsCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_BATCH_SIZE)
			{
				options.MaxBatchSize = MAX_ALLOWABLE_BATCH_SIZE;
			}

			return MultipleRowsCopy2Async(new DB2iSeriesMultipleRowsHelper<T>(table, options), source, " FROM " + Constants.SQL.DummyTableName(), cancellationToken);
		}

#if !NETFRAMEWORK
		protected override Task<BulkCopyRowsCopied> MultipleRowsCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_BATCH_SIZE)
			{
				options.MaxBatchSize = MAX_ALLOWABLE_BATCH_SIZE;
			}

			return MultipleRowsCopy2Async(new DB2iSeriesMultipleRowsHelper<T>(table, options), source, " FROM " + Constants.SQL.DummyTableName(), cancellationToken);
		}
#endif
	}
}
