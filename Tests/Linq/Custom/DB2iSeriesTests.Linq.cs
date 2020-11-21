using LinqToDB;
using LinqToDB.Data;
using NUnit.Framework;
using System;
using System.Linq;
using Tests.Model;

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void TestAny([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>();

				Assert.True(person.Any(p => p.ID == 2));
				Assert.False(person.Any(p => p.ID == 23));
				Assert.True(person.Any(p => !(p.ID == 23)));
			}
		}

		[Test]
		public void TestOrderBySkipTake([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>().OrderBy(p => p.LastName).Skip(2).Take(2);

				var results = person.ToArray();

				Assert.AreEqual(2, results.Count());
				Assert.AreEqual("Pupkin", results.First().LastName);
				Assert.AreEqual("Testerson", results.Last().LastName);
			}
		}

		[Test]
		public void TestOrderByDescendingSkipTake([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>().OrderByDescending(p => p.LastName).Skip(2).Take(2);

				var results = person.ToArray();

				Assert.AreEqual(2, results.Count());
				Assert.AreEqual("König", results.First().LastName);
				Assert.AreEqual("Doe", results.Last().LastName);
			}
		}

		[Test]
		public void CompareDate1([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = GetDataContext(context))
			{
				var expected = Types.Where(t => t.ID == 1 && t.DateTimeValue <= DateTime.Today);

				var actual = db.Types.Where(t => t.ID == 1 && t.DateTimeValue <= DateTime.Today);

				Assert.AreEqual(expected, actual);
			}
		}

		[Test]
		public void CompareDate2([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			var dt = Types2[3].DateTimeValue;

			using (var db = GetDataContext(context))
			{
				var expected = Types2.Where(t => t.DateTimeValue.Value.Date > dt.Value.Date);
				var actual = db.Types2.Where(t => t.DateTimeValue.Value.Date > dt.Value.Date);

				AreEqual(expected, actual);
			}
		}
	}
}
