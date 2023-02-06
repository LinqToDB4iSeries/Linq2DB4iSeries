using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using LinqToDB;
using System.Linq;
using FluentAssertions;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;


namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void ZeroPad([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var v = db.Person.FirstOrDefault(x => Sql.ZeroPad(x.ID, 3) == "001");

				Assert.NotNull(v);
			}
		}

		[Test]
		public void Substr([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var v = db.Person.FirstOrDefault(x => Sql.Substring(x.FirstName, 2, 2) == "oh");

				Assert.NotNull(v);
			}
		}

		[Test]
		public void TrimLeft([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var v = db.Person.FirstOrDefault(x => Sql.TrimLeft(" " + x.FirstName) == x.FirstName);

				Assert.NotNull(v);
			}
		}

		[Test]
		public void TrimRight([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var v = db.Person.FirstOrDefault(x => Sql.TrimLeft(x.FirstName + " ") == x.FirstName);

				Assert.NotNull(v);
			}
		}

		[Test]
		public void Truncate([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var v = db.Person.FirstOrDefault(x => Sql.Truncate(1.134) == x.ID);

				Assert.NotNull(v);
			}
		}
	}
}
