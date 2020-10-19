using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Data;
	using LinqToDB;
	using LinqToDB.Common;
	using DB2BulkCopyOptions = DB2.DB2ProviderAdapter.DB2BulkCopyOptions;
	using LinqToDB.Tools;

	class DB2iSeriesBulkCopy : BasicBulkCopy
	{
		const int MAX_ALLOWABLE_BATCH_SIZE = 100;

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
				if (dataProvider.ProviderType == DB2iSeriesAdoProviderType.DB2
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter adapter)
				{
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
						return ProviderSpecificCopyImpl_DB2(
							table,
							options,
							source,
							dataConnection,
							connection,
							adapter.BulkCopy,
							TraceAction);
				}
			}

			return MultipleRowsCopy(table, options, source);
		}

		protected override Task<BulkCopyRowsCopied> ProviderSpecificCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType == DB2iSeriesAdoProviderType.DB2
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter adapter)
				{
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
						// call the synchronous provider-specific implementation
						return Task.FromResult(ProviderSpecificCopyImpl_DB2(
							table,
							options,
							source,
							dataConnection,
							connection,
							adapter.BulkCopy,
							TraceAction));
				}
			}

			return MultipleRowsCopyAsync(table, options, source, cancellationToken);
		}

#if !NETFRAMEWORK
		protected override async Task<BulkCopyRowsCopied> ProviderSpecificCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (table.DataContext is DataConnection dataConnection)
			{
				if (dataProvider.ProviderType == DB2iSeriesAdoProviderType.DB2
					&& dataProvider.Adapter.WrappedAdapter is DB2.DB2ProviderAdapter adapter)
				{
					var connection = dataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema);
					if (connection != null)
					{
						var enumerator = source.GetAsyncEnumerator(cancellationToken);
						await using (enumerator.ConfigureAwait(Common.Configuration.ContinueOnCapturedContext))
						{
							// call the synchronous provider-specific implementation
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

		//Copied from DB2BulkCopy
		private static BulkCopyRowsCopied ProviderSpecificCopyImpl_DB2<T>(
			ITable<T> table,
			BulkCopyOptions options,
			IEnumerable<T> source,
			DataConnection dataConnection,
			IDbConnection connection,
			DB2.DB2ProviderAdapter.BulkCopyAdapter bulkCopy,
			Action<DataConnection, Func<string>, Func<int>> traceAction)
		{
			var descriptor = dataConnection.MappingSchema.GetEntityDescriptor(typeof(T));
			var columns = descriptor.Columns.Where(c => !c.SkipOnInsert || options.KeepIdentity == true && c.IsIdentity).ToList();
			var rd = new BulkCopyReader<T>(dataConnection, columns, source);
			var rc = new BulkCopyRowsCopied();
			var sqlBuilder = dataConnection.DataProvider.CreateSqlBuilder(dataConnection.MappingSchema);
			var tableName = GetTableName(sqlBuilder, options, table);

			var bcOptions = DB2BulkCopyOptions.Default;

			if (options.KeepIdentity == true) bcOptions |= DB2BulkCopyOptions.KeepIdentity;
			if (options.TableLock == true) bcOptions |= DB2BulkCopyOptions.TableLock;

			using (var bc = bulkCopy.Create(connection, bcOptions))
			{
				var notifyAfter = options.NotifyAfter == 0 && options.MaxBatchSize.HasValue ?
					options.MaxBatchSize.Value : options.NotifyAfter;

				if (notifyAfter != 0 && options.RowsCopiedCallback != null)
				{
					bc.NotifyAfter = notifyAfter;

					bc.DB2RowsCopied += (sender, args) =>
					{
						rc.RowsCopied = args.RowsCopied;
						options.RowsCopiedCallback(rc);
						if (rc.Abort)
							args.Abort = true;
					};
				}

				if (options.BulkCopyTimeout.HasValue)
					bc.BulkCopyTimeout = options.BulkCopyTimeout.Value;
				else if (Configuration.Data.BulkCopyUseConnectionCommandTimeout)
					bc.BulkCopyTimeout = connection.ConnectionTimeout;

				bc.DestinationTableName = tableName;

				for (var i = 0; i < columns.Count; i++)
					bc.ColumnMappings.Add(bulkCopy.CreateColumnMapping(i, sqlBuilder.ConvertInline(columns[i].ColumnName, SqlProvider.ConvertType.NameToQueryField)));

				traceAction(
					dataConnection,
					() => "INSERT BULK " + tableName + Environment.NewLine,
					() => { bc.WriteToServer(rd); return rd.Count; });
			}

			if (rc.RowsCopied != rd.Count)
			{
				rc.RowsCopied = rd.Count;

				if (options.NotifyAfter != 0 && options.RowsCopiedCallback != null)
					options.RowsCopiedCallback(rc);
			}

			return rc;
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
