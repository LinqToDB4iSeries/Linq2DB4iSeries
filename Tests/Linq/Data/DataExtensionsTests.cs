using System;
using System.Linq;

using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.DB2iSeries;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;
using NUnit.Framework;
using Tests.Model;

namespace Tests.Data
{
	[TestFixture]
	public class DataExtensionsTests : TestBase
	{


		class QueryObject
		{
			public int Column1;
			public DateTime Column2;
		}


		[Test]
		public void TestObject3()
		{
			var arr1 = new byte[] { 48, 57 };
			var arr2 = new byte[] { 42 };

			using (var conn = new DataConnection())
			{
			    var param = ParamType(conn, SqlDataType.GetDataType(typeof(byte[])));
			    var sql = $"SELECT cast(@P as {param}){Base.Helpers.GetDummyFrom(conn.DataProvider)}";
				Assert.That(conn.Execute<byte[]>(sql, new { p = arr1 }), Is.EqualTo(arr1));
				Assert.That(conn.Execute<byte[]>(sql, new { p = arr2 }), Is.EqualTo(arr2));
			}
		}

	    private static string ParamType(DataConnection conn, SqlDataType type)
	    {
	        var bldr = (DB2iSeriesSqlBuilder) conn.DataProvider.CreateSqlBuilder();
	        var param = bldr.GetiSeriesType(type);
	        return param;
	    }

	    [Test]
		public void TestObject4()
		{
			using (var conn = new DataConnection())
			{
			    var param = ParamType(conn, new SqlDataType(DataType.Int64));
                var sql = $"SELECT cast(@P as {param}){Base.Helpers.GetDummyFrom(conn.DataProvider)}";
				Assert.That(conn.Execute<int>(sql, new { p = 1 }), Is.EqualTo(1));
			}
		}

		[Test]
		public void TestObject5()
		{
			using (var conn = new DataConnection())
			{
			    var param = ParamType(conn, new SqlDataType(DataType.Int64));
                var sql = $"SELECT cast(@P as {param}){Base.Helpers.GetDummyFrom(conn.DataProvider)}";

				var res = conn.Execute<string>(
					sql,
					new
					{
						p = new DataParameter { DataType = DataType.VarChar, Value = "123" },
						p1 = 1
					});

				Assert.That(res, Is.EqualTo("123"));
			}
		}

		[Test, DataContextSource(false)]
		public void TestObject51(string context)
		{
			using (var conn = new TestDataConnection(context))
			{
				var sql = conn.Person.Where(p => p.ID == 1).Select(p => p.Name).Take(1).ToString().Replace("-- Access", "");
				var res = conn.Execute<string>(sql);

				Assert.That(res, Is.EqualTo("John"));
			}
		}

		[Test]
		public void TestObject6()
		{
			using (var conn = new DataConnection())
			{
			    var param = ParamType(conn, new SqlDataType(DataType.Int64));
                string sql = $"SELECT cast(@P as {param}){Base.Helpers.GetDummyFrom(conn.DataProvider)}";

				Assert.That(conn.Execute<string>(
					sql,
					new
					{
						p1 = new DataParameter { Name = "p", DataType = DataType.Char, Value = "123" },
						p2 = 1
					}), Is.EqualTo("123"));
			}
		}

		[ScalarType(false)]
		struct QueryStruct
		{
			public int Column1;
			public DateTime Column2;
		}



		[ScalarType]
		class TwoValues
		{
			public int Value1;
			public int Value2;
		}

#pragma warning disable 675

		//[Test, Ignore("Needs the appropriate mapping schema and value converter instantiating.")]
		public void TestDataParameterMapping1()
		{
			var ms = new MappingSchema();
			

			ms.SetConvertExpression<TwoValues, DataParameter>(tv => new DataParameter { Value = (long)tv.Value1 << 32 | tv.Value2 });

			using (var conn = new DataConnection().AddMappingSchema(ms))
			{
			    var param = ParamType(conn, new SqlDataType(DataType.Int64));
                string sql = $"SELECT cast(@P as {param}){Base.Helpers.GetDummyFrom(conn.DataProvider)}";

				var n = conn.Execute<long>(sql, new { p = new TwoValues { Value1 = 1, Value2 = 2 } });

				Assert.AreEqual(1L << 32 | 2, n);
			}
		}

		//[Test, Ignore("Needs the appropriate mapping schema and value converter instantiating.")]
		public void TestDataParameterMapping2()
		{
			var ms = new MappingSchema();

			ms.SetConvertExpression<TwoValues, DataParameter>(tv => new DataParameter { Value = (long)tv.Value1 << 32 | tv.Value2 });

			using (var conn = new DataConnection().AddMappingSchema(ms))
			{
			    var param = ParamType(conn, new SqlDataType(DataType.Int64));
                string sql = $"SELECT cast(@P as {param}){Base.Helpers.GetDummyFrom(conn.DataProvider)}";

				var n = conn.Execute<long?>(sql, new { p = (TwoValues)null });

				Assert.AreEqual(null, n);
			}
		}

		//[Test, Ignore("Needs the appropriate mapping schema and value converter instantiating.")]
		public void TestDataParameterMapping3()
		{
			var ms = new MappingSchema();

			ms.SetConvertExpression<TwoValues, DataParameter>(tv =>
				 new DataParameter
				 {
					 Value = tv == null ? (long?)null : (long)tv.Value1 << 32 | tv.Value2,
					 DataType = DataType.Int64
				 },
				false);

			using (var conn = new DataConnection().AddMappingSchema(ms))
			{
			    var param = ParamType(conn, new SqlDataType(DataType.Int64));
                string sql = $"SELECT cast(@P as {param}){Base.Helpers.GetDummyFrom(conn.DataProvider)}";

				var n = conn.Execute<long?>(sql, new { p = (TwoValues)null });

				Assert.AreEqual(null, n);
			}
		}
	}
}
