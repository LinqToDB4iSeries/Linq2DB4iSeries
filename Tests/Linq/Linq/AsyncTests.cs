using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.SqlQuery;

using NUnit.Framework;

namespace Tests.Linq
{
	using LinqToDB.Tools;
	using Model;

	[TestFixture]
	public class AsyncTests : TestBase
	{
		[Test, DataContextSource(false)]
		public void Test(string context)
		{
			TestImpl(context);
		}

		async void TestImpl(string context)
		{
			Test1(context);

			using (var db = GetDataContext(context + ".LinqService"))
			{
				var list = await db.Parent.ToArrayAsync();
				Assert.That(list.Length, Is.Not.EqualTo(0));
			}
		}

		[Test, DataContextSource(false)]
		public void Test1(string context)
		{
			using (var db = GetDataContext(context + ".LinqService"))
			{
				var list = db.Parent.ToArrayAsync().Result;
				Assert.That(list.Length, Is.Not.EqualTo(0));
			}
		}

		[Test, DataContextSource(false)]
		public void TestForEach(string context)
		{
			TestForEachImpl(context);
		}

		async void TestForEachImpl(string context)
		{
			using (var db = GetDataContext(context + ".LinqService"))
			{
				var list = new List<Parent>();

				await db.Parent.ForEachAsync(list.Add);

				Assert.That(list.Count, Is.Not.EqualTo(0));
			}
		}

		[Test, DataContextSource(false)]
		public void TestExecute1(string context)
		{
			TestExecute1Impl(context);
		}

		async void TestExecute1Impl(string context)
		{
			using (var conn = new TestDataConnection(context))
			{
				var sql = conn.Person.Where(p => p.ID == 1).Select(p => p.Name).Take(1).ToString().Replace("-- Access", "");

				var res = await conn.SetCommand(sql).ExecuteAsync<string>();

				Assert.That(res, Is.EqualTo("John"));
			}
		}

		[Test, DataContextSource(false)]
		public void TestExecute2(string context)
		{
			using (var conn = new TestDataConnection(context))
			{
				var sql = conn.Person.Where(p => p.ID == 1).Select(p => p.Name).Take(1).ToString().Replace("-- Access", "");

				var res = conn.SetCommand(sql).ExecuteAsync<string>().Result;

				Assert.That(res, Is.EqualTo("John"));
			}
		}

		[Test, DataContextSource(false)]
		public void TestQueryToArray(string context)
		{
			TestQueryToArrayImpl(context);
		}

		async void TestQueryToArrayImpl(string context)
		{
			using (var conn = new TestDataConnection(context))
			{
				var sql = conn.Person.Where(p => p.ID == 1).Select(p => p.Name).Take(1).ToString().Replace("-- Access", "");

				using (var rd = await conn.SetCommand(sql).ExecuteReaderAsync())
				{
					var list = await rd.QueryToArrayAsync<string>();

					Assert.That(list[0], Is.EqualTo("John"));
				}
			}
		}

		[Test, DataContextSource]
		public async Task FirstAsyncTest(string context)
		{
			using (var db = GetDataContext(context))
			{
				var person = await db.Person.FirstAsync(p => p.ID == 1);

				Assert.That(person.ID, Is.EqualTo(1));
			}
		}

		[Test, DataContextSource]
		public async Task ContainsAsyncTest(string context)
		{
			using (var db = GetDataContext(context))
			{
				var p = new Person { ID = 1 };

				var r = await db.Person.ContainsAsync(p);

				Assert.That(r, Is.True);
			}
		}

		[Test, DataContextSource]
		public async Task TestFirstOrDefault(string context)
		{
			using (var db = GetDataContext(context))
			{
				var param = 4;
				var resultQuery =
					from o in db.Parent
					where Sql.Ext.In(o.ParentID, -1, -2, -3) || o.ParentID == param
					select o;

				var zz = await resultQuery.FirstOrDefaultAsync();
			}
		}
	}

	class MyItemBuilder : Sql.IExtensionCallBuilder
	{
		public void Build(Sql.ISqExtensionBuilder builder)
		{
			var values = builder.GetValue<System.Collections.IEnumerable>("values");
			builder.Query.IsParameterDependent = true;

			foreach (var value in values)
			{
				var param = new SqlParameter(value?.GetType() ?? typeof(int?), "p", value);
				builder.AddParameter("values", param);
			}
		}
	}

	public static class MySqlExtensions
	{
		[Sql.Extension("{field} IN ({values, ', '})", IsPredicate = true, BuilderType = typeof(MyItemBuilder))]
		public static bool In<T>(this Sql.ISqlExtension ext, [ExprParameter] T field, params T[] values)
		{
			throw new NotImplementedException();
		}
	}
}

