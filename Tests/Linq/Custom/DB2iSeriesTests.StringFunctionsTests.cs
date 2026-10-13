//Edited tests copied from linq2db 

using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using NUnit.Framework;
using System.Linq;
using static Tests.Linq.StringFunctionsTests;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Table]
		sealed class SampleClass
		{
			[PrimaryKey] public int Id { get; set; }
			[Column(Length = 50, CanBeNull = true)] public string? Value1 { get; set; }
			[Column(Length = 50, CanBeNull = true)] public string? Value2 { get; set; }
			[Column(Length = 50, CanBeNull = true, DataType = DataType.VarChar)] public string? Value3 { get; set; }
			[Column(Length = 50, CanBeNull = true, DataType = DataType.NVarChar)] public string? Value4 { get; set; }
		}

		private static SampleClass[] GenerateData()
		{
			var data = new[]
			{
				new SampleClass { Id = 1, Value1 = "V1", Value2 = "V2", Value4 = "V4" },
				new SampleClass { Id = 2, Value1 = null, Value2 = "Z2", Value4 = null },
				new SampleClass { Id = 3, Value1 = "Z1", Value2 = null, Value4 = "Z4" }
			};
			return data;
		}

		[Test]
		public void AggregationSelectorTest([StringTestSources] string context)
		{
			var data = GenerateData();

			// https://github.com/ClickHouse/ClickHouse/issues/29978
			// if it changes, CanBeNull = false should be removed from mappings
			var nullVal = context.IsAnyOf(TestProvName.AllClickHouse) ? string.Empty : null;

			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable(data);
			var actual =
				from t in table
				group t by new { t.Id, Value = t.Value1 }
				into g
				orderby g.Key.Id
				select new
				{
					Values = g.StringAggregate(" -> ", e => e.Value1).OrderBy(x => x.Value1).ToValue(),
				};

			var expected =
				from t in data
				group t by new { t.Id, Value = t.Value1 }
				into g
				orderby g.Key.Id
				select new
				{
					Values = Sql.ConcatStringsNullable(" -> ", g.Select(e => e.Value1)) ?? nullVal,
				};

			AreEqual(expected, actual);
		}

		[Test]
		public void AggregationTest([StringTestSources] string context)
		{
			var data = GenerateData();

			// https://github.com/ClickHouse/ClickHouse/issues/29978
			// if it changes, CanBeNull = false should be removed from mappings
			var nullVal = context.IsAnyOf(TestProvName.AllClickHouse) ? string.Empty : null;

			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable(data);
			var actual =
				from t in table
				group t.Value1 by new { t.Id, Value = t.Value1 }
				into g
				orderby g.Key.Id
				select new
				{
					Max = g.Max(),
					Values = g.StringAggregate(" -> ").OrderBy(x => x).ToValue(),
				};

			var expected =
				from t in data
				group t.Value1 by new { t.Id, Value = t.Value1 }
				into g
				orderby g.Key.Id
				select new
				{
					Max = g.Max(),
					Values = Sql.ConcatStringsNullable(" -> ", g) ?? nullVal,
				};

			AreEqual(expected, actual);
		}

		[Test]
		public void Issue1765TestLiteral4([StringTestSources] string context)
		{
			var data = GenerateData();

			// https://github.com/ClickHouse/ClickHouse/issues/29978
			// if it changes, CanBeNull = false should be removed from mappings
			var nullVal = context.IsAnyOf(TestProvName.AllClickHouse) ? string.Empty : null;

			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable(data);
			var actual =
				from t in table
				group t.Value4 by new { t.Id, Value = t.Value4 }
				into g
				orderby g.Key.Id
				select new
				{
					Max = g.Max(),
					Values = g.StringAggregate(" -> ").OrderBy(x => x).ToValue(),
				};

			var expected =
				from t in data
				group t.Value4 by new { t.Id, Value = t.Value4 }
				into g
				orderby g.Key.Id
				select new
				{
					Max = g.Max(),
					Values = Sql.ConcatStringsNullable(" -> ", g) ?? nullVal,
				};

			AreEqual(expected, actual);
		}

		[Test]
		public void Issue1765TestParameter4([StringTestSources] string context, [Values(" -> ", " => ")] string separator)
		{
			var data = GenerateData();

			// https://github.com/ClickHouse/ClickHouse/issues/29978
			// if it changes, CanBeNull = false should be removed from mappings
			var nullVal = context.IsAnyOf(TestProvName.AllClickHouse) ? string.Empty : null;

			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable(data);
			var actual =
				from t in table
				group t.Value4 by new { t.Id, Value = t.Value4 }
				into g
				orderby g.Key.Id
				select new
				{
					Max = g.Max(),
					Values = g.StringAggregate(separator).OrderBy(x => x).ToValue(),
				};

			var expected =
				from t in data
				group t.Value4 by new { t.Id, Value = t.Value4 }
				into g
				orderby g.Key.Id
				select new
				{
					Max = g.Max(),
					Values = Sql.ConcatStringsNullable(separator, g) ?? nullVal,
				};

			AreEqual(expected, actual);
		}

		[Test(Description = "https://github.com/linq2db/linq2db/issues/4501")]
		public void Issue4501Test([StringTestSources] string context)
		{
			var data = GenerateData();

			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable(data);

			var query = table
				.GroupBy(x => x.Id)
				.Select(g => new
				{
					Id = g.Key,
					AggregatedDescription = g.StringAggregate(", ", x => x.Value1).OrderBy(x => x.Value1).ToValue()
				});

			query.ToList();
		}
	}
}
