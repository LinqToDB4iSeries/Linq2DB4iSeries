using System;
using System.Linq;

using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;

using NUnit.Framework;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		sealed class DateTimeOffsetCoverageRow
		{
			[PrimaryKey] public int Id { get; set; }
			// Leave one property inferred to exercise the default CLR type mapping.
			[Column] public DateTimeOffset Value { get; set; }
			[Column(DataType = DataType.DateTimeOffset, Precision = 6)] public DateTimeOffset? NullableValue { get; set; }
			[Column(DataType = DataType.DateTimeOffset, Precision = 0)] public DateTimeOffset WholeSeconds { get; set; }
		}

		static DateTimeOffset OffsetCoverageValue(int offsetMinutes) =>
			new DateTimeOffset(2024, 2, 29, 12, 34, 56, TimeSpan.FromMinutes(offsetMinutes)).AddTicks(1234560);

		sealed class DateTimeOffsetStoredRow
		{
			[Column] public int Id { get; set; }
			[Column] public DateTime Value { get; set; }
		}

		static void AssertOffsetWallClock(DateTimeOffset actual, DateTimeOffset expected)
		{
			// IBM i TIMESTAMP stores wall-clock components, without the original offset.
			// Do not compare instants: materialization can attach the client's local offset.
			Assert.That(actual.DateTime, Is.EqualTo(expected.DateTime));
		}

		[Test]
		public void DateTimeOffsetEntityRoundTrip(
			[IncludeDataSources(TestProvNameDb2i.All)] string context,
			[Values(false, true)] bool inline,
			[Values(0, 330, -420)] int offsetMinutes)
		{
			using var db = GetDataContext(context);
			db.InlineParameters = inline;
			using var table = db.CreateLocalTable<DateTimeOffsetCoverageRow>();
			var value = OffsetCoverageValue(offsetMinutes);
			var whole = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerSecond));
			db.Insert(new DateTimeOffsetCoverageRow { Id = 1, Value = value, NullableValue = value, WholeSeconds = whole });
			db.Insert(new DateTimeOffsetCoverageRow { Id = 2, Value = whole, NullableValue = null, WholeSeconds = whole });

			var rows = table.OrderBy(r => r.Id).ToArray();
			AssertOffsetWallClock(rows[0].Value, value);
			Assert.That(rows[0].NullableValue.HasValue, Is.True);
			AssertOffsetWallClock(rows[0].NullableValue!.Value, value);
			AssertOffsetWallClock(rows[0].WholeSeconds, whole);
			AssertOffsetWallClock(rows[1].Value, whole);
			Assert.That(rows[1].NullableValue, Is.Null);

			// Verify the stored components independently of DateTimeOffset materialization.
			var stored = db.GetTable<DateTimeOffsetStoredRow>().TableName(table.TableName);
			Assert.That(stored.Where(r => r.Id == 1).Select(r => r.Value).Single(), Is.EqualTo(value.DateTime));
		}

		[Test]
		public void DateTimeOffsetCapturedParameters(
			[IncludeDataSources(TestProvNameDb2i.All)] string context,
			[Values(false, true)] bool inline)
		{
			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable<DateTimeOffsetCoverageRow>();
			var value = OffsetCoverageValue(330);
			db.Insert(new DateTimeOffsetCoverageRow { Id = 1, Value = value, NullableValue = value, WholeSeconds = value.DateTime.Date });
			db.Insert(new DateTimeOffsetCoverageRow { Id = 2, Value = value.AddDays(1), WholeSeconds = value.DateTime.Date });
			db.InlineParameters = inline;

			Assert.That(table.Where(r => r.Value == value).Select(r => r.Id).ToArray(), Is.EqualTo(new[] { 1 }));
			DateTimeOffset? nullable = value;
			Assert.That(table.Where(r => r.NullableValue == nullable).Select(r => r.Id).ToArray(), Is.EqualTo(new[] { 1 }));
			nullable = null;
			Assert.That(table.Where(r => r.NullableValue == nullable).Select(r => r.Id).ToArray(), Is.EqualTo(new[] { 2 }));
			var later = value.AddTicks(10);
			Assert.That(table.Where(r => r.Value >= later).Select(r => r.Id).ToArray(), Is.EqualTo(new[] { 2 }));

			// Equal wall clocks with different offsets serialize identically; equal instants need not.
			var sameClock = new DateTimeOffset(value.DateTime, TimeSpan.FromHours(-7));
			Assert.That(table.Count(r => r.Value == sameClock), Is.EqualTo(1));
			var sameInstant = value.ToOffset(TimeSpan.Zero);
			Assert.That(table.Count(r => r.Value == sameInstant), Is.Zero);
			AssertOffsetWallClock(table.Select(r => Sql.AsSql(value)).First(), value);
		}

		[Test]
		public void DateTimeOffsetExplicitParameters(
			[IncludeDataSources(TestProvNameDb2i.All)] string context,
			[Values(0, 330, -420)] int offsetMinutes)
		{
			using var db = new DataConnection(context);
			var value = OffsetCoverageValue(offsetMinutes);
			AssertOffsetWallClock(db.ExecuteScalarParameter<DateTimeOffset>("p", "timestamp(6)", value), value);
			AssertOffsetWallClock(db.ExecuteScalarParameter<DateTimeOffset>(
				new DataParameter("p", value, DataType.DateTimeOffset), "timestamp(6)"), value);
			var nullable = db.ExecuteScalarParameter<DateTimeOffset?>(
				new DataParameter("p", value, DataType.DateTimeOffset), "timestamp(6)");
			Assert.That(nullable.HasValue, Is.True);
			AssertOffsetWallClock(nullable!.Value, value);
			Assert.That(db.ExecuteScalarParameter<DateTimeOffset?>(
				new DataParameter("p", null, DataType.DateTimeOffset), "timestamp(6)"), Is.Null);
		}

		[Test]
		public void DateTimeOffsetUpdates(
			[IncludeDataSources(TestProvNameDb2i.All)] string context,
			[Values(false, true)] bool inline)
		{
			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable<DateTimeOffsetCoverageRow>();
			var value = OffsetCoverageValue(-420);
			var row = new DateTimeOffsetCoverageRow { Id = 1, Value = value, WholeSeconds = value.DateTime.Date };
			db.Insert(row);
			db.InlineParameters = inline;
			row.Value = value.AddDays(1);
			row.NullableValue = value;
			db.Update(row);
			var actual = table.Single();
			AssertOffsetWallClock(actual.Value, row.Value);
			Assert.That(actual.NullableValue.HasValue, Is.True);
			AssertOffsetWallClock(actual.NullableValue!.Value, value);

			var updated = value.AddDays(2);
			table.Where(r => r.Id == 1).Set(r => r.Value, updated).Set(r => r.NullableValue, (DateTimeOffset?)null).Update();
			actual = table.Single();
			AssertOffsetWallClock(actual.Value, updated);
			Assert.That(actual.NullableValue, Is.Null);
		}
	}
}
