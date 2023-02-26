#nullable disable

using LinqToDB;
using LinqToDB.Configuration;
using LinqToDB.Data;
using LinqToDB.DataProvider.DB2iSeries;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void DataOptionsFromProviderOptions([DataSources] string context)
		{
			var o = new DataOptions().UseConfigurationString(context);
			using var db1 = new TestDataConnection(o);
			var cs = db1.ConnectionString;

			var p = db1.GetTable<Person>().First(x => x.ID == 1);

			Assert.AreEqual(1, p.ID);

			o = new DataOptions().UseDB2iSeries(cs);

			using var db2 = new TestDataConnection(o);

			db2.EnsureConnection(true);

			p = db2.GetTable<Person>().First(x => x.ID == 1);

			Assert.AreEqual(1, p.ID);
		}
	}
}
