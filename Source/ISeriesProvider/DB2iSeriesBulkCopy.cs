using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
		
	partial class DB2iSeriesBulkCopy : BasicBulkCopy
	{
		const int MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE = 100;

		private readonly DB2iSeriesDataProvider dataProvider;

		public DB2iSeriesBulkCopy(DB2iSeriesDataProvider dataProvider)
		{
			this.dataProvider = dataProvider;
		}

		protected override BulkCopyRowsCopied ProviderSpecificCopy<T>(
			ITable<T> table,
			BulkCopyOptions options,
			IEnumerable<T> source)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType.IsDB2()
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter db2Adapter)
				{
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
						return ProviderSpecificCopyImpl_DB2(
							table,
							options,
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
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
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

		protected override Task<BulkCopyRowsCopied> ProviderSpecificCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType.IsDB2()
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter db2Adapter)
				{
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
						// call the synchronous provider-specific implementation as provider doesn't support async
						return Task.FromResult(ProviderSpecificCopyImpl_DB2(
							table,
							options,
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
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
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

#if !NETFRAMEWORK
		protected override async Task<BulkCopyRowsCopied> ProviderSpecificCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType == DB2iSeriesProviderType.DB2
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter adapter)
				{
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
					{
						var enumerator = source.GetAsyncEnumerator(cancellationToken);
						await using (enumerator.ConfigureAwait(Common.Configuration.ContinueOnCapturedContext))
						{
							// call the synchronous provider-specific implementation as provider doesn't support async
							return ProviderSpecificCopyImpl_DB2(
								table,
								options,
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

		protected override BulkCopyRowsCopied MultipleRowsCopy<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source)
		{
			if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE)
			{
				options.MaxBatchSize = MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE;
			}

			return MultipleRowsCopy2(new DB2iSeriesMultipleRowsHelper<T>(table, options), source, " FROM " + Constants.SQL.DummyTableName());
		}

		protected override Task<BulkCopyRowsCopied> MultipleRowsCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE)
			{
				options.MaxBatchSize = MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE;
			}

			return MultipleRowsCopy2Async(new DB2iSeriesMultipleRowsHelper<T>(table, options), source, " FROM " + Constants.SQL.DummyTableName(), cancellationToken);
		}

#if !NETFRAMEWORK
		protected override Task<BulkCopyRowsCopied> MultipleRowsCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			if ((options.MaxBatchSize ?? int.MaxValue) > MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE)
			{
				options.MaxBatchSize = MAX_ALLOWABLE_MULTIPLE_ROWS_BATCH_SIZE;
			}

			return MultipleRowsCopy2Async(new DB2iSeriesMultipleRowsHelper<T>(table, options), source, " FROM " + Constants.SQL.DummyTableName(), cancellationToken);
		}
#endif
	}
}
