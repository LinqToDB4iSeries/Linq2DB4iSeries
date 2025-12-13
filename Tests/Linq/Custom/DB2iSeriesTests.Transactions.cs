using LinqToDB.DataProvider.DB2iSeries;
using LinqToDB.DataProvider.SqlServer;
using NUnit.Framework;
using System.Threading.Tasks;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests : TestBase
	{
		[Test(Description = "https://github.com/linq2db/linq2db/issues/5225")]
		public async Task AttachToExistingTransaction([DataSources(false)] string context)
		{
			var connectionString = GetConnectionString(context);

			using var db = GetDataConnection(context);
			using var tr = db.BeginTransaction();

			DB2iSeriesTools.GetDataProvider(connectionString: connectionString, connection: db.OpenDbConnection(), transaction: db.Transaction);
		}
	}
}
