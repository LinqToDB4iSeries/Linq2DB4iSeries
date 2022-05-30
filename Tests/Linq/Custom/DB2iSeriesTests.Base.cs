using LinqToDB;
using LinqToDB.Mapping;
using NUnit.Framework;
using System.Linq;
using System.ServiceModel;
using Tests.Model;

#nullable disable

namespace Tests.DataProvider
{
	[TestFixture]
	public partial class DB2iSeriesTests : TestBase
	{
		[Table("LinqDataTypes")]
		class TestTable
		{
			[Column("ID")]
			public int ID { get; set; }
		}

		// for SAP HANA cross-server queries see comments how to configure SAP HANA in TestUtils.GetServerName() method
		[Test]
		public void Issue681_TestTableFQN(
			[DataSources] string context,
			[Values] bool withServer,
			[Values] bool withDatabase,
			[Values] bool withSchema)
		{
			var throws = false;

			string serverName;
			string schemaName;
			string dbName;

			using (new DisableBaseline("Use instance name is SQL", false))
			using (var db = GetDataContext(context, testLinqService: false))
			{
				if (withDatabase && !withSchema)
				{
					throws = true;
				}

				using (new DisableLogging())
				{
					serverName = withServer ? db.Select(() => ServerName()) : null;
					dbName = withDatabase ? db.GetTable<LinqDataTypes>().Select(_ => DbName()).First() : null;
					schemaName = withSchema ? db.GetTable<LinqDataTypes>().Select(_ => SchemaName()).First() : null;
				}

				var table = db.GetTable<TestTable>();

				if (withServer) table = table.ServerName(serverName!);
				if (withDatabase) table = table.DatabaseName(dbName!);
				if (withSchema) table = table.SchemaName(schemaName!);

				if (throws && context.Contains(".LinqService"))
				{
#if NET472
					Assert.Throws<FaultException<ExceptionDetail>>(() => table.ToList());
#endif
				}
				else if (throws)
				{
					Assert.Throws<LinqToDBException>(() => table.ToList());
				}
				else
					table.ToList();
			}
		}

		[Test]
		public void Issue825_Test([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var userId = 32;
				var childId = 32;

				//Configuration.Linq.OptimizeJoins = false;

				var query = db.GetTable<UserTests.Issue825Tests.Parent825>()
					.Where(p => p.ParentPermissions.Any(perm => perm.UserId == userId))
					.SelectMany(parent => parent.Childs)
					.Where(child => child.Id == childId)
					.Select(child => child.Parent);

				var result = query.ToList();

				Assert.AreEqual(1, result.Count);
				Assert.AreEqual(3, result[0].Id);
			}
		}

		[Table]
		class TestTrun
		{
			[Column, PrimaryKey] public int ID;
			[Column] public decimal Field1;
		}
	}
}
