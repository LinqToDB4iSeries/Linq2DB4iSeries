using NUnit.Framework;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using System;
using System.Data;
using System.Linq;
using System.ServiceModel;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests : TestBase
	{
		[Test]
		public void TestProviderAutoDetect([DataSources(false)] string context)
		{
			var connectionString = GetConnectionString(context);

			// .UseConfiguration(context)
			// used to improve detection as using only connection string and assembly sniffing
			// doesn't work good for some providers
			using var db = new DataConnection(new DataOptions().UseConfiguration(context).UseDB2iSeries(connectionString));

			db.GetTable<Person>().ToArray();
		}
	}
}
