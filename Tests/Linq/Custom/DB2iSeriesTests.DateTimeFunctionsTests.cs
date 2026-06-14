using LinqToDB;
using NUnit.Framework;
using System;
using System.Linq;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void GetDate([DataSources] string context)
		{
			using (new DisableBaseline("Server-side date generation test"))
			using (var db = GetDataContext(context))
			{
				var q = from p in db.Person where p.ID == 1 select new { Now = Sql.AsSql(Sql.GetDate()) };
				var sqlNow = q.First().Now;

				Assert.That(q.ToList().First().Now.Year, Is.EqualTo(DateTime.Now.Year));
			}
		}

		[Test]
		public void CurrentTimestamp([DataSources] string context, [Values] bool inline)
		{
			using (new DisableBaseline("Server-side date generation test"))
			using (var db = GetDataContext(context))
			{
				db.InlineParameters = inline;

				var q = from p in db.Person where p.ID == 1 select new { Now = Sql.CurrentTimestamp };
				var sqlNow = q.First().Now;

				Assert.That(q.ToList().First().Now.Year, Is.EqualTo(DateTime.Now.Year));
			}
		}


		[Test]
		public void CurrentTimestamp2([DataSources] string context, [Values] bool inline)
		{
			using (new DisableBaseline("Server-side date generation test"))
			using (var db = GetDataContext(context))
			{
				db.InlineParameters = inline;

				var q = from p in db.Person where p.ID == 1 select new { Now = Sql.CurrentTimestamp2 };
				var sqlNow = q.First().Now;

				Assert.That(q.ToList().First().Now.Year, Is.EqualTo(DateTime.Now.Year));
			}
		}

		[Test]
		public void DateTimeOffsetNow([DataSources] string context, [Values] bool inline)
		{
			using (new DisableBaseline("Server-side date generation test"))
			using (var db = GetDataContext(context))
			{
				db.InlineParameters = inline;

				var row = db.Person.Where(p => p.ID == 1)
					.Select(_ => Sql.AsSql(DateTimeOffset.Now))
					.Select(v => new { Full = v, v.Year, v.Month, v.Day, v.Hour, v.Minute, v.Second })
					.First();

				// ClickHouse, PGSQL: session timezone used when set explicitly for connection
				// MySql/MariaDB, YDB: returns UTC
				// Oracle: Extract for TSTZ use UTC value
				var returnsUtc = context.IsAnyOf(
					TestProvName.AllPostgreSQL,
					TestProvName.AllClickHouse,
					TestProvName.AllMySql,
					TestProvName.AllOracle,
					ProviderName.Ydb);
				var kind = returnsUtc
					? DateTimeKind.Utc
					: DateTimeKind.Local;

				var dbParts = new DateTime(row.Year, row.Month, row.Day, row.Hour, row.Minute, row.Second, kind);
				var now = DateTimeOffset.Now;

				// Component check — what the server actually generated, no ADO.NET TZ coercion.
				// Most providers return local time + local offset → components match local wall-clock.
				// Postgres / ClickHouse / Ydb normalize to UTC internally → components are UTC.
				var expectedParts = returnsUtc ? DateTime.UtcNow : DateTime.Now;

				Assert.That(
					(expectedParts - dbParts).Duration().TotalMinutes, Is.LessThan(5),
					$"expected {expectedParts:O}, db parts {dbParts:O}");

				// Round-trip instant matches on every provider — on plain-timestamp providers
				// ADO.NET attaches the client's local offset, which equals the server's offset.
				Assert.That(
					(now - row.Full).Duration().TotalMinutes, Is.LessThan(5),
					$"{now}, {row.Full}");

				// Offset preserved on TZ-aware-non-normalized providers
				// Oracle: see above
				if ((returnsUtc && !context.IsAnyOf(TestProvName.AllOracle)) || context.IsAnyOf(TestProvName.AllDuckDB))
					Assert.That(row.Full.Offset, Is.EqualTo(TimeSpan.Zero));
				else
					Assert.That(row.Full.Offset, Is.EqualTo(now.Offset));
			}
		}

		[Test]
		public void DateTimeOffsetNowUtc([DataSources] string context, [Values] bool inline)
		{
			using (new DisableBaseline("Server-side date generation test"))
			using (var db = GetDataContext(context))
			{
				db.InlineParameters = inline;

				var row = db.Person.Where(p => p.ID == 1)
					.Select(_ => Sql.AsSql(DateTimeOffset.UtcNow))
					.Select(v => new { Full = v, v.Year, v.Month, v.Day, v.Hour, v.Minute, v.Second })
					.First();

				var dbParts = new DateTime(row.Year, row.Month, row.Day, row.Hour, row.Minute, row.Second);
				var nowUtc = DateTime.UtcNow;
				var now = DateTimeOffset.UtcNow;

				// Components are always UTC for UtcNow on every provider — authoritative correctness check.
				Assert.That(
					(nowUtc - dbParts).Duration().TotalMinutes, Is.LessThan(5),
					$"client UTC {nowUtc:O}, db parts {dbParts:O}");

				Assert.That(
					(nowUtc - row.Full.DateTime).Duration().TotalMinutes, Is.LessThan(5),
					$"client UTC {nowUtc:O}, db full.DateTime {row.Full.DateTime:O}");
			}
		}
	}
}
