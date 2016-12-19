using System;
using System.Linq;

using LinqToDB.Data;
using LinqToDB.DataProvider.SqlServer;
using LinqToDB.Mapping;
using NUnit.Framework;

namespace Tests.SchemaProvider
{
	using LinqToDB;

	[TestFixture]
	public class SchemaProviderTests : TestBase
	{
		[Test, DataContextSource(false)]
		public void Test(string context)
		{
			//TODO: tries to load all the columns for all the tables
			Assert.Inconclusive("Needs re-writing");

			//SqlServerTools.ResolveSqlTypes("");

			//using (var conn = new DataConnection(context))
			//{
			//	var sp = conn.DataProvider.GetSchemaProvider();
			//	var dbSchema = sp.GetSchema(conn);

			//	//dbSchema.Tables.ToDictionary(
			//	//  t => t.IsDefaultSchema ? t.TableName : t.SchemaName + "." + t.TableName,
			//	//  t => t.Columns.ToDictionary(c => c.ColumnName));

			//	var table = dbSchema.Tables.SingleOrDefault(t => t.TableName.ToLower() == "parent");

			//	Assert.That(table, Is.Not.Null);
			//	Assert.That(table.Columns.Count(c => c.ColumnName != "_ID"), Is.EqualTo(2));

			//	//				Assert.That(dbSchema.Tables.Single(t => t.TableName.ToLower() == "doctor").ForeignKeys.Count, Is.EqualTo(1));

			//}
		}

		class PKTest
		{
			[PrimaryKey(1)]
			public int ID1;
			[PrimaryKey(2)]
			public int ID2;
		}

		[Test, DataContextSource]
		public void DB2iSeriesTest(string context)
		{
			//TODO: tries to load all the columns for all the tables
			Assert.Inconclusive("Needs re-writing");

			//using (var conn = new DataConnection(context))
			//{
			//	var sp = conn.DataProvider.GetSchemaProvider();
			//	var dbSchema = sp.GetSchema(conn);
			//	var table = dbSchema.Tables.Single(t => t.TableName == "ALLTYPES");
			//	Assert.That(table.Columns.Single(c => c.ColumnName == "BINARYDATATYPE").ColumnType, Is.EqualTo("CHAR (5) FOR BIT DATA"));
			//	Assert.That(table.Columns.Single(c => c.ColumnName == "VARBINARYDATATYPE").ColumnType, Is.EqualTo("VARCHAR (5) FOR BIT DATA"));
			//}
		}
	}
}
