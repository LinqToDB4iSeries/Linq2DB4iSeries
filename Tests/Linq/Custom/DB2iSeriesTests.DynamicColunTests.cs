//Edited tests copied from linq2db 

using System;
using System.Collections.Generic;
using System.Linq;

using LinqToDB;
using LinqToDB.DataProvider.Firebird;
using LinqToDB.Mapping;

using NUnit.Framework;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests : TestBase
	{
		[Table("DynamicTable")]
		public class DynamicTablePrototype
		{
			[Column, Identity, PrimaryKey]
			public int ID { get; set; }

			[Column("NotIdentifier")] 
			public int NotIdentifier { get; set; }

			[Column("SomeValue")] 
			public int Value { get; set; }
		}

		[Table("DynamicTable")]
		public class DynamicTable
		{
			[Column, Identity, PrimaryKey]
			public int ID { get; set; }
		}

		[Test]
		public void SqlPropertyNoStoreNonIdentifier([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			using (db.CreateLocalTable(new[]
			{
				new DynamicTablePrototype { NotIdentifier = 77 }
			}))
			{
				var query =
					from d in db.GetTable<DynamicTable>()
					select new
					{
						NI = Sql.Property<int>(d, "NotIdentifier")
					};

				var result = query.ToArray();

				Assert.AreEqual(77, result[0].NI);
			}
		}

		[Test]
		public void SqlPropertyNoStoreNonIdentifierGrouping([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			using (db.CreateLocalTable(new[]
			{
				new DynamicTablePrototype { NotIdentifier = 77, Value = 5 },
				new DynamicTablePrototype { NotIdentifier = 77, Value = 5 }
			}))
			{
				var query =
					from d in db.GetTable<DynamicTable>()
					group d by new { NI = Sql.Property<int>(d, "NotIdentifier") }
					into g
					select new
					{
						g.Key.NI,
						Count = g.Count(),
						Sum = g.Sum(i => Sql.Property<int>(i, "SomeValue"))
					};

				var result = query.ToArray();

				Assert.AreEqual(77, result[0].NI);
				Assert.AreEqual(2, result[0].Count);
				Assert.AreEqual(10, result[0].Sum);
			}
		}
	}
}
