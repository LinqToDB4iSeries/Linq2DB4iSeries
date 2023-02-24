using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
		
	partial class DB2iSeriesBulkCopy : BasicBulkCopy
	{
		const int MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE = 100;

		/// <remarks>
		/// Settings based on https://www.ibm.com/docs/en/i/7.3?topic=reference-sql-limits
		/// We subtract 1 here to be safe since some ADO providers use parameter for command itself.
		/// </remarks>
		protected override int MaxParameters => 1999;
		/// <remarks>
		/// Setting based on https://www.ibm.com/docs/en/i/7.3?topic=reference-sql-limits
		/// Max is actually 2MIB, but we keep a lower number here to avoid the cost of huge statements.
		/// </remarks>
		protected override int MaxSqlLength => 327670;

		private readonly DB2iSeriesDataProvider dataProvider;
		private readonly DB2iSeriesSqlProviderFlags dB2ISeriesSqlProviderFlags;

		public DB2iSeriesBulkCopy(DB2iSeriesDataProvider dataProvider, DB2iSeriesSqlProviderFlags dB2ISeriesSqlProviderFlags)
		{
			this.dataProvider = dataProvider;
			this.dB2ISeriesSqlProviderFlags = dB2ISeriesSqlProviderFlags;
		}

		protected override BulkCopyRowsCopied ProviderSpecificCopy<T>(
			ITable<T> table,
			DataOptions options,
			IEnumerable<T> source)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType.IsDB2()
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter db2Adapter)
				{
					if (dataProvider.TryGetProviderConnection(dataConnection, out var connection))
						return ProviderSpecificCopyImpl_DB2(
							table,
							options.BulkCopyOptions,
							source,
							dataConnection,
							connection,
							db2Adapter.BulkCopy,
							TraceAction);
				}
#if NETFRAMEWORK
				else if (dataProvider.ProviderType.IsAccessClient()
					&& dataProvider.Adapter.WrappedAdapter is DB2iSeriesAccessClientProviderAdapter idb2Adapter)
				{
					if (dataProvider.TryGetProviderConnection(dataConnection, out var connection))
						// call the synchronous provider-specific implementation
						return ProviderSpecificCopyImpl_AccessClient(
							table,
							options,
							source,
							dataConnection,
							connection,
							idb2Adapter,
							TraceAction);
				}
#endif
			}
			return MultipleRowsCopy(table, options, source);
		}

		protected override Task<BulkCopyRowsCopied> ProviderSpecificCopyAsync<T>(ITable<T> table, DataOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType.IsDB2()
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter db2Adapter)
				{
					if (dataProvider.TryGetProviderConnection(dataConnection, out var connection))
						// call the synchronous provider-specific implementation as provider doesn't support async
						return Task.FromResult(ProviderSpecificCopyImpl_DB2(
							table,
							options.BulkCopyOptions,
							source,
							dataConnection,
							connection,
							db2Adapter.BulkCopy,
							TraceAction));
				}
#if NETFRAMEWORK
				else if (dataProvider.ProviderType.IsAccessClient()
					&& dataProvider.Adapter.WrappedAdapter is DB2iSeriesAccessClientProviderAdapter idb2Adapter)
				{
					if (dataProvider.TryGetProviderConnection(dataConnection, out var connection))
						// call the synchronous provider-specific implementation as provider doesn't support async
						return Task.FromResult(ProviderSpecificCopyImpl_AccessClient(
							table,
							options,
							source,
							dataConnection,
							connection,
							idb2Adapter,
							TraceAction));
				}
#endif
			}

			return MultipleRowsCopyAsync(table, options, source, cancellationToken);
		}

#if NATIVE_ASYNC
		protected override async Task<BulkCopyRowsCopied> ProviderSpecificCopyAsync<T>(ITable<T> table, DataOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType == DB2iSeriesProviderType.DB2
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter adapter)
				{
					if (dataProvider.TryGetProviderConnection(dataConnection, out var connection))
					{
						var enumerator = source.GetAsyncEnumerator(cancellationToken);
						await using (enumerator.ConfigureAwait(Common.Configuration.ContinueOnCapturedContext))
						{
							// call the synchronous provider-specific implementation as provider doesn't support async
							return ProviderSpecificCopyImpl_DB2(
								table,
								options.BulkCopyOptions,
								AsyncToSyncEnumerable(enumerator),
								dataConnection,
								connection,
								adapter.BulkCopy,
								TraceAction);
						}
					}
				}
			}

			return await MultipleRowsCopyAsync(table, options, source, cancellationToken).ConfigureAwait(Common.Configuration.ContinueOnCapturedContext);
		}

		private static IEnumerable<T> AsyncToSyncEnumerable<T>(IAsyncEnumerator<T> enumerator)
		{
			while (enumerator.MoveNextAsync().GetAwaiter().GetResult())
			{
				yield return enumerator.Current;
			}
		}
#endif
		private int GetMultipleRowsMaxBatchSize(DataOptions options)
		{
			var maxBatchSize = options.BulkCopyOptions.MaxBatchSize ?? int.MaxValue;

			return maxBatchSize > MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE ? MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE : maxBatchSize;
		}

		protected override BulkCopyRowsCopied MultipleRowsCopy<T>(ITable<T> table, DataOptions options, IEnumerable<T> source)
		{
			return MultipleRowsCopy2(new DB2iSeriesMultipleRowsHelper<T>(table, options, dB2ISeriesSqlProviderFlags) { BatchSize = GetMultipleRowsMaxBatchSize(options) }, source, " FROM " + Constants.SQL.DummyTableName());
		}

		protected override Task<BulkCopyRowsCopied> MultipleRowsCopyAsync<T>(ITable<T> table, DataOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			return MultipleRowsCopy2Async(new DB2iSeriesMultipleRowsHelper<T>(table, options, dB2ISeriesSqlProviderFlags) { BatchSize = GetMultipleRowsMaxBatchSize(options) }, source, " FROM " + Constants.SQL.DummyTableName(), cancellationToken);
		}

#if NATIVE_ASYNC
		protected override Task<BulkCopyRowsCopied> MultipleRowsCopyAsync<T>(ITable<T> table, DataOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			return MultipleRowsCopy2Async(new DB2iSeriesMultipleRowsHelper<T>(table, options, dB2ISeriesSqlProviderFlags) { BatchSize = GetMultipleRowsMaxBatchSize(options) }, source, " FROM " + Constants.SQL.DummyTableName(), cancellationToken);
		}
#endif
	}
}
