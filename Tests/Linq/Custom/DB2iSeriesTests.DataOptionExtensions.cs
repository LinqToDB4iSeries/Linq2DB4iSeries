using LinqToDB;
using NUnit.Framework;
using System.Linq;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void DataOptionsFromProviderOptions([DataSources] string context)
		{
			var o = new DataOptions().UseConfiguration(context);
			using var db1 = new TestDataConnection(o);
			var cs = db1.ConnectionString;

			var p = db1.GetTable<Person>().First(x => x.ID == 1);

			Assert.That(p.ID, Is.EqualTo(1));

			o = new DataOptions().UseDB2iSeries(cs!);

			using var db2 = new TestDataConnection(o);

			db2.OpenDbConnection();

			p = db2.GetTable<Person>().First(x => x.ID == 1);

			Assert.That(p.ID, Is.EqualTo(1));
		}
	}
}
