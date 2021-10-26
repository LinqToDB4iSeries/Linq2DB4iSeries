using LinqToDB;
using NUnit.Framework;
using System.Linq;
using Tests.Model;

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void InsertOrUpdateWithIntegers([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = new TestDataConnection(context))
			{
				LinqToDB.ITable<MergeTypesByte> table;
				using (new DisableLogging())
				{
					db.DropTable<MergeTypesByte>(throwExceptionIfNotExists: false);
					table = db.CreateTable<MergeTypesByte>();
				}

				ulong val = long.MaxValue;

				table.InsertOrUpdate(
					() => new MergeTypesByte { FieldByte = 27, FieldULong = val },
					s => new MergeTypesByte { FieldByte = 27, FieldULong = val },
					() => new MergeTypesByte { FieldByte = 22, FieldULong = val }
				);

				Assert.AreEqual(1, table.Count());
			}
		}
	}
}
