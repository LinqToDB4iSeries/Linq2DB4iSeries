using LinqToDB.Data;
using NUnit.Framework;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void EnumExecuteScalarTest([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var dbm = new TestDataConnection(context))
			{
				var gender = dbm.Execute<Gender>("SELECT 'M' FROM SYSIBM.SYSDUMMY1");

				Assert.That(gender, Is.EqualTo(Gender.Male));
			}
		}
	}
}
