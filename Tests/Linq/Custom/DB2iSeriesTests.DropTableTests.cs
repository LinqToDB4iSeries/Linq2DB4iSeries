using LinqToDB;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		class DropTableTest
		{
			public int ID { get; set; }
		}

		[Test]
		public void DropSpecificDatabaseTableTest([DataSources(false, TestProvName.AllSapHana)] string context)
		{
			using (var db = new TestDataConnection(context))
			{
				// cleanup
				db.DropTable<DropTableTest>(throwExceptionIfNotExists: false);

				var schema = db.ExecuteScalar<string>("CURRENT_SCHEMA");
				var database = db.ExecuteScalar<string>("CURRENT_SERVER");

				var table = db.CreateTable<DropTableTest>()
					.SchemaName(schema)
					.DatabaseName(database);


				table.Insert(() => new DropTableTest() { ID = 123 });

				var data = table.ToList();

				Assert.That(data, Is.Not.Null);
				Assert.That(data.Count, Is.EqualTo(1));
				Assert.That(data[0].ID, Is.EqualTo(123));

				table.Drop();

				var sql = db.LastQuery!;

				// check that table dropped
				var exception = Assert.Catch(() => table.ToList());
				Assert.That(exception is Exception, Is.True);

				// TODO: we need better assertion here
				// Right now we just check generated sql query, not that it is
				// executed properly as we use only one test database
				if (database != TestUtils.NO_DATABASE_NAME)
					Assert.That(sql.Contains(database), Is.True);

				if (schema != TestUtils.NO_SCHEMA_NAME)
					Assert.That(sql.Contains(schema), Is.True);
			}
		}
	}
}
