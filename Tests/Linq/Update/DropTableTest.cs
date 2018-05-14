using LinqToDB;
using NUnit.Framework;
using System;
using System.Linq;
using Tests.Model;

namespace Tests.xUpdate
{
	[TestFixture]
	public class DropTableTests : TestBase
	{
		class DropTableTest
		{
			public int ID { get; set; }
		}

		[Test, DataContextSource]
		public void DropCurrentDatabaseTableTest(string context)
		{
			using (var db = GetDataContext(context))
			{
				// cleanup
				db.DropTable<DropTableTest>(throwExceptionIfNotExists: false);

				var table = db.CreateTable<DropTableTest>();

				table.Insert(() => new DropTableTest() { ID = 123 });

				var data = table.ToList();

				Assert.NotNull(data);
				Assert.AreEqual(1, data.Count);
				Assert.AreEqual(123, data[0].ID);

				table.Drop();

				// check that table dropped
				var exception = Assert.Catch(() => table.ToList());
				Assert.True(exception is Exception);
			}
		}
	}
}
