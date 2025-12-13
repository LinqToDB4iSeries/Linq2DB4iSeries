using LinqToDB;
using LinqToDB.Mapping;
using NUnit.Framework;
using LinqToDB.DataProvider.DB2iSeries;
using System.Linq;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Table]
		sealed class OverridingSystemValueTable
		{
			[Column(DataType = DataType.Int32), PrimaryKey, NotNull, Identity] public int Id { get; set; }
			[Column(DataType = DataType.VarChar)] public string Name { get; set; } = null!;
		}

		[Test]
		public void OverridingSystemValueTest([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using var db = GetDataContext(context);
			using var tb = db.CreateLocalTable<OverridingSystemValueTable>();

			tb.Insert(() => new OverridingSystemValueTable { Name = "Test1" });
			
			tb.TableHint(DB2iSeriesHints.Table.OverridingSystemValue)
				.Insert(() => new OverridingSystemValueTable { Id = 1000, Name = "Test2" });

			var actual1 = tb.ToList();

			tb.TableHint(DB2iSeriesHints.Table.OverridingSystemValue)
				.Where(x => x.Id == 1000)
				.Update(x => new OverridingSystemValueTable { Id = 1001, Name = "Updated" });

			var actual2 = tb.ToList();

			using (Assert.EnterMultipleScope())
			{
				Assert.That(actual1.First().Id, Is.EqualTo(1));
				Assert.That(actual1.Last().Id, Is.EqualTo(1000));
				Assert.That(actual2.First().Id, Is.EqualTo(1));
				Assert.That(actual2.Last().Id, Is.EqualTo(1001));
			}
		}
	}
}
