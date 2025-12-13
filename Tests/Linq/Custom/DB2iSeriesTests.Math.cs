using LinqToDB;
using NUnit.Framework;
using System.Linq;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void MathLogTests_Double([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = GetDataContext(context))
				AreEqual(
					from t in from p in Types select Sql.Log((double)p.MoneyValue, 2d) where t != 0.1 select t,
					from t in from p in db.Types select Sql.Log((double)p.MoneyValue, 2d) where t != 0.1 select t);
		}

		[Test]
		public void MathLogTests_Decimal([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = GetDataContext(context))
				AreEqual(
					from t in from p in Types select Sql.Log((double)p.MoneyValue, 2d) where t != 0.1 select t,
					from t in from p in db.Types select Sql.Log((double)p.MoneyValue, 2d) where t != 0.1 select t);
		}
	}
}
