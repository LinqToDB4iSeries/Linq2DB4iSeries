using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Tools;
	using System.Diagnostics;
	using System.Data.Common;

	partial class DB2iSeriesBulkCopy : BasicBulkCopy
	{
		private const int MAX_ACCESS_CLIENT_BATCH_SIZE = 10000;

		private BulkCopyRowsCopied ProviderSpecificCopyImpl_AccessClient<T>(
			ITable<T> table,
			DataOptions dataOptions,
			IEnumerable<T> source,
			DataConnection dataConnection,
			DbConnection connection,
			DB2iSeriesAccessClientProviderAdapter adapter,
			Action<DataConnection, Func<string>, Func<int>> traceAction)
		{
			var options = dataOptions.BulkCopyOptions;
			var descriptor = dataConnection.MappingSchema.GetEntityDescriptor(typeof(T));
			var columns = descriptor.Columns.Where(c => !c.SkipOnInsert || options.KeepIdentity == true && c.IsIdentity).ToList();
			var rd = new BulkCopyReader<T>(dataConnection, columns, source);
			var rc = new BulkCopyRowsCopied();
			var sqlBuilder = dataConnection.DataProvider.CreateSqlBuilder(table.DataContext.MappingSchema, dataOptions);
			var tableName = GetTableName(sqlBuilder, options, table);

			var columnNames = columns.Select(x => x.ColumnName);
			var fields = string.Join(", ", columnNames);
			var parameters = string.Join(", ", columnNames.Select(x => "@" + x));

			var sql = $"INSERT INTO {tableName} ({fields}) VALUES({parameters})";

			var batchSize = Math.Min(options.MaxBatchSize ?? MAX_ACCESS_CLIENT_BATCH_SIZE, MAX_ACCESS_CLIENT_BATCH_SIZE);
			var sw = Stopwatch.StartNew();

			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText = sql;
				adapter.DeriveParameters(cmd);

				var columnDataTypes = cmd.Parameters.Cast<DbParameter>()
					.Select(adapter.GetDbType)
					.Select(adapter.GetDataType)
					.ToList();

				//*LOB and XML types not supported, fallback to multiple rows
				if (columnDataTypes.Any(x => x.In(DataType.Blob, DataType.Text, DataType.NText, DataType.Xml)))
					return MultipleRowsCopy(table, dataOptions, source);

				var columnDbDataTypes = columns
					.Select((x,i) => x
						.GetDbDataType(true)
						.WithDataType(columnDataTypes[i]))
					.ToList();

				var count = 0;
				var notificationBatchCount = 0;
				var bufferEmpty = false;
				while (rd.Read())
				{
					var i = 0;
					bufferEmpty = false;
					foreach (DbParameter parameter in cmd.Parameters)
					{
						dataConnection.DataProvider.SetParameter(
							dataConnection, 
							parameter, 
							columns[i].ColumnName,
							columnDbDataTypes[i], 
							rd.GetValue(i++));
					}

					try
					{
						adapter.AddBatch(cmd);
					}
					//In case columns can't be mapped fall back to multiple rows
					catch
					{
						return MultipleRowsCopy(table, dataOptions, source);
					}

					count++;
					notificationBatchCount++;

					if (count % batchSize == 0)
					{
						execute();
						if (rc.Abort) return rc;
						bufferEmpty = true;
					}
				}

				if (!bufferEmpty)
				{
					execute();
					if (rc.Abort) return rc;
				}

				void execute()
				{
					cmd.ExecuteNonQuery();

					rc.RowsCopied = count;

					if (sw.Elapsed.TotalSeconds > options.BulkCopyTimeout)
					{
						traceAction(
							dataConnection,
							() => $"INSERT BULK {tableName} TIMEOUT AFTER {sw.Elapsed.TotalSeconds} SECONDS" + Environment.NewLine,
							() => (int)rc.RowsCopied);

						rc.Abort = true;

						return;
					}

					if (options.NotifyAfter > 0 && notificationBatchCount > options.NotifyAfter)
					{
						options.RowsCopiedCallback?.Invoke(rc);
						notificationBatchCount = 0;
					}

					if (rc.Abort)
					{
						traceAction(
							dataConnection,
							() => $"INSERT BULK {tableName} ABORTED BY USER" + Environment.NewLine,
							() => (int)rc.RowsCopied);

						return;
					}
				}
			}

			return rc;
		}
	}
}
