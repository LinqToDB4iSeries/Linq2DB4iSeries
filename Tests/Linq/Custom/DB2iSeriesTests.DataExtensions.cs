//Edited tests copied from linq2db 

using LinqToDB;
using LinqToDB.Data;
using NUnit.Framework;
using System;
using System.Linq;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void TestObject3()
		{
			var arr1 = new byte[] { 48, 57 };
			var arr2 = new byte[] { 42 };

			using (var conn = new DataConnection())
			{
				Assert.That(conn.Execute<byte[]>("VALUES (CAST(? AS BINARY(2)))", new { p = arr1 }), Is.EqualTo(arr1));
				Assert.That(conn.Execute<byte[]>("VALUES (CAST(? AS BINARY(1)))", new { p = arr2 }), Is.EqualTo(arr2));
			}
		}

		[Test]
		public void TestObject4()
		{
			using (var conn = new DataConnection())
			{
				Assert.That(conn.Execute<int>("VALUES (CAST(? AS INTEGER))", new { p = 1 }), Is.EqualTo(1));
			}
		}

		[Test]
		public void TestObject5()
		{
			using (var conn = new DataConnection())
			{
				var res = conn.Execute<string>(
				"VALUES (CAST(? AS VARCHAR(3)))",
				new
				{
					p = new DataParameter { DataType = DataType.VarChar, Value = "123" }
				});

				Assert.That(res, Is.EqualTo("123"));
			}
		}

		[Test]
		public void TestObject51([DataSources(false)] string context)
		{
			using (var conn = new TestDataConnection(context))
			{
				conn.InlineParameters = true;
				var sql = conn.Person.Where(p => p.ID == 1).Select(p => p.Name).Take(1).ToString()!;
				sql = string.Join(Environment.NewLine, sql.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
				.Where(line => !line.StartsWith("--")));
				var res = conn.Execute<string>(sql);

				Assert.That(res, Is.EqualTo("John"));
			}
		}

		[Test]
		public void TestObjectProjection([DataSources(false)] string context)
		{
			using (var conn = new TestDataConnection(context))
			{
				var result = conn.Person.Where(p => p.ID == 1).Select(p => new { p.ID, p.Name })
				.Take(1)
				.ToArray();

				var expected = Person.Where(p => p.ID == 1).Select(p => new { p.ID, p.Name })
				.Take(1)
				.ToArray();

				AreEqual(expected, result);
			}
		}

		[Test]
		public void TestObject6()
		{
			using (var conn = new DataConnection())
			{
				Assert.That(conn.Execute<string>(
				"VALUES (CAST(? AS CHAR(3)))",
				new
				{
					p1 = new DataParameter { Name = "p", DataType = DataType.Char, Value = "123" }
				}), Is.EqualTo("123"));
			}
		}
	}
}
