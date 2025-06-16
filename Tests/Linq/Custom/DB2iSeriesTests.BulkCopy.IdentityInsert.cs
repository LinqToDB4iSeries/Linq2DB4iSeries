//Edited tests copied from linq2db 

using System;
using System.Linq;

using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;

using NUnit.Framework;

namespace Tests.DataProvider
{
	using LinqToDB.DataProvider.Informix;
	using Model;
	using System.Collections.Generic;
	using System.Threading.Tasks;

	[TestFixture]
	[Order(10000)]
	public partial class DB2iSeriesTests : TestBase
	{
		[Table("KeepIdentityTest")]
		public class TestTable1
		{
			[Identity]
			public int ID { get; set; }

			[Column("intDataType")]
			[Column("Value", Configuration = ProviderName.DB2)]
			[Column("Value", Configuration = ProviderName.Sybase)]
			public int Value { get; set; }
		}

		[Table("KeepIdentityTest")]
		public class TestTable2
		{
			[Identity, Column(SkipOnInsert = true)]
			public int ID { get; set; }

			[Column("intDataType")]
			[Column("Value", Configuration = ProviderName.DB2)]
			[Column("Value", Configuration = ProviderName.Sybase)]
			public int Value { get; set; }
		}

		[Test]
		public async Task KeepIdentity_SkipOnInsertTrue(
			[IncludeDataSources(false, TestProvNameDb2i.All)] string context,
			[Values(null, true, false)] bool? keepIdentity,
			[Values] BulkCopyType copyType,
			[Values(0, 1, 2)] int asyncMode) // 0 == sync, 1 == async, 2 == async with IAsyncEnumerable
		{
			ResetAllTypesIdentity(context);

			// don't use transactions as some providers will fallback to non-provider-specific implementation then
			using (var db = new TestDataConnection(context))
			{
				var lastId = db.InsertWithInt32Identity(new TestTable2());
				try
				{
					var options = new BulkCopyOptions()
					{
						KeepIdentity = keepIdentity,
						BulkCopyType = copyType
					};

					if (!await ExecuteAsync(db, context, perform, keepIdentity, copyType))
						return;

					var data = db.GetTable<TestTable2>().Where(_ => _.ID > lastId).OrderBy(_ => _.ID).ToArray();

					Assert.That(data.Length, Is.EqualTo(2));

					// oracle supports identity insert only starting from version 12c, which is not used yet for tests
					var useGenerated = keepIdentity != true;

					Assert.That(data[0].ID, Is.EqualTo(lastId + (!useGenerated ? 10 : 1)));
					Assert.That(data[0].Value, Is.EqualTo(200));
					Assert.That(data[1].ID, Is.EqualTo(lastId + (!useGenerated ? 20 : 2)));
					Assert.That(data[1].Value, Is.EqualTo(300));

					async Task perform()
					{
						var values = new[]
							{
								new TestTable2()
								{
									ID = lastId + 10,
									Value = 200
								},
								new TestTable2()
								{
									ID = lastId + 20,
									Value = 300
								}
							};
						if (asyncMode == 0) // synchronous 
						{
							db.BulkCopy(
								options,
								values);
						}
						else if (asyncMode == 1) // asynchronous
						{
							await db.BulkCopyAsync(
								options,
								values);
						}
						else // asynchronous with IAsyncEnumerable
						{
							await db.BulkCopyAsync(
								options,
								AsAsyncEnumerable(values));
						}
					}
				}
				finally
				{
					// cleanup
					db.GetTable<TestTable2>().Delete(_ => _.ID >= lastId);
				}
			}
		}

		[Test]
		public async Task KeepIdentity_SkipOnInsertFalse(
			[IncludeDataSources(false, TestProvNameDb2i.All)] string context,
			[Values(null, true, false)] bool? keepIdentity,
			[Values] BulkCopyType copyType,
			[Values(0, 1, 2)]           int          asyncMode) // 0 == sync, 1 == async, 2 == async with IAsyncEnumerable
		{
			ResetAllTypesIdentity(context);

			// don't use transactions as some providers will fallback to non-provider-specific implementation then
			using (var db = new TestDataConnection(context))
			{
				var lastId = db.InsertWithInt32Identity(new TestTable1());
				try
				{
					var options = new BulkCopyOptions()
					{
						KeepIdentity = keepIdentity,
						BulkCopyType = copyType
					};

					if (!await ExecuteAsync(db, context, perform, keepIdentity, copyType))
						return;

					var data = db.GetTable<TestTable1>().Where(_ => _.ID > lastId).OrderBy(_ => _.ID).ToArray();

					Assert.That(data.Length, Is.EqualTo(2));

					var useGenerated = keepIdentity != true;

					Assert.That(data[0].ID, Is.EqualTo(lastId + (!useGenerated ? 10 : 1)));
					Assert.That(data[0].Value, Is.EqualTo(200));
					Assert.That(data[1].ID, Is.EqualTo(lastId + (!useGenerated ? 20 : 2)));
					Assert.That(data[1].Value, Is.EqualTo(300));

					async Task perform()
					{
						var values = new[]
							{
								new TestTable1()
								{
									ID = lastId + 10,
									Value = 200
								},
								new TestTable1()
								{
									ID = lastId + 20,
									Value = 300
								}
							};
						if (asyncMode == 0) // synchronous
						{
							db.BulkCopy(
								options,
								values);
						}
						else if (asyncMode == 1) // asynchronous
						{
							await db.BulkCopyAsync(
								options,
								values);
						}
						else // asynchronous with IAsyncEnumerable
						{
							await db.BulkCopyAsync(
								options,
								AsAsyncEnumerable(values));
						}
					}
				}
				finally
				{
					// cleanup
					db.GetTable<TestTable1>().Delete(_ => _.ID >= lastId);
				}
			}
		}

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
		private async IAsyncEnumerable<T> AsAsyncEnumerable<T>(IEnumerable<T> enumerable)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
		{
			var enumerator = enumerable.GetEnumerator();
			while (enumerator.MoveNext())
			{
				yield return enumerator.Current;
			}
		}

		private async Task<bool> ExecuteAsync(DataConnection db, string context, Func<Task> perform, bool? keepIdentity, BulkCopyType copyType)
		{
			// RowByRow right now uses DataConnection.Insert which doesn't support identity insert
			if (copyType == BulkCopyType.RowByRow && keepIdentity == true)
			{
				var ex = Assert.CatchAsync(async () => await perform());
				Assert.That(ex, Is.InstanceOf<LinqToDBException>());
				Assert.That(ex!.Message, Is.EqualTo("BulkCopyOptions.KeepIdentity = true is not supported by BulkCopyType.RowByRow mode"));
				return false;
			}

			if (keepIdentity == true)
			{
				var ex = Assert.CatchAsync(async () => await perform());
				//Assert.IsInstanceOf<LinqToDBException>(ex);
				Assert.That(ex!.Message.Contains("GENERATED ALWAYS"), Is.True);
				return false;
			}

			await perform();
			return true;
		}
	}
}
