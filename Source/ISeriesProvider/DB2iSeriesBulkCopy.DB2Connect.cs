﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Data;
	using LinqToDB;
	using LinqToDB.Common;
	using DB2BulkCopyOptions = DB2.DB2ProviderAdapter.DB2BulkCopyOptions;

	internal partial class DB2iSeriesBulkCopy : BasicBulkCopy
	{
		//Copied from DB2BulkCopy
		private static BulkCopyRowsCopied ProviderSpecificCopyImpl_DB2<T>(
		ITable<T> table,
		BulkCopyOptions options,
		IEnumerable<T> source,
		DataConnection dataConnection,
		DbConnection connection,
		DB2.DB2ProviderAdapter.BulkCopyAdapter bulkCopy,
		Action<DataConnection, Func<string>, Func<int>> traceAction)
		{
			var descriptor = table.DataContext.MappingSchema.GetEntityDescriptor(typeof(T), dataConnection.Options.ConnectionOptions.OnEntityDescriptorCreated);
			var columns = descriptor.Columns.Where(c => !c.SkipOnInsert || options.KeepIdentity == true && c.IsIdentity).ToList();
			using var rd = new BulkCopyReader<T>(dataConnection, columns, source);
			var rc = new BulkCopyRowsCopied();
			var sqlBuilder = dataConnection.DataProvider.CreateSqlBuilder(table.DataContext.MappingSchema, dataConnection.Options);
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

					bc.DB2RowsCopied += (_, args) =>
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

			if (table.DataContext.CloseAfterUse)
				table.DataContext.Close();

			return rc;
		}
	}
}
