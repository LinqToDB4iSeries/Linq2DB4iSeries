using System;
using System.Linq;

using LinqToDB.Data;
using LinqToDB.DataProvider.SqlServer;
using LinqToDB.Mapping;
using NUnit.Framework;

namespace Tests.SchemaProvider
{
	using System.Collections.Generic;
	using LinqToDB;
	using LinqToDB.SchemaProvider;

	[TestFixture]
	public class SchemaProviderTests : TestBase
	{
		[Test, DataContextSource(false)]
		public void Test(string context)
		{
			SqlServerTools.ResolveSqlTypes("");

			using (var conn = new DataConnection(context))
			{
				var sp = conn.DataProvider.GetSchemaProvider();
				var dbSchema = sp.GetSchema(conn);

				var tableNames = new HashSet<string>();
				foreach (var schemaTable in dbSchema.Tables)
				{
					var tableName = schemaTable.CatalogName + "."
									+ (schemaTable.IsDefaultSchema
										? schemaTable.TableName
										: schemaTable.SchemaName + "." + schemaTable.TableName);

					if (tableNames.Contains(tableName))
						Assert.Fail("Not unique table " + tableName);

					tableNames.Add(tableName);

					var columnNames = new HashSet<string>();
					foreach (var schemaColumm in schemaTable.Columns)
					{
						if (columnNames.Contains(schemaColumm.ColumnName))
							Assert.Fail("Not unique column {0} for table {1}.{2}", schemaColumm.ColumnName, schemaTable.SchemaName, schemaTable.TableName);

						columnNames.Add(schemaColumm.ColumnName);
					}
				}

				var table = dbSchema.Tables.SingleOrDefault(t => t.TableName.ToLower() == "parent");

				Assert.That(table, Is.Not.Null);
				Assert.That(table.Columns.Count(c => c.ColumnName != "_ID"), Is.EqualTo(2));

				AssertType<Model.LinqDataTypes>(conn.MappingSchema, dbSchema);
				AssertType<Model.Parent>(conn.MappingSchema, dbSchema);

				//				Assert.That(dbSchema.Tables.Single(t => t.TableName.ToLower() == "doctor").ForeignKeys.Count, Is.EqualTo(1));

			}
		}

		static void AssertType<T>(MappingSchema mappingSchema, DatabaseSchema dbSchema)
		{
			var e = mappingSchema.GetEntityDescriptor(typeof(T));

			var schemaTable = dbSchema.Tables.FirstOrDefault(_ => _.TableName.Equals(e.TableName, StringComparison.OrdinalIgnoreCase));
			Assert.IsNotNull(schemaTable, e.TableName);

			Assert.That(schemaTable.Columns.Count >= e.Columns.Count);

			foreach (var column in e.Columns)
			{
				var schemaColumn = schemaTable.Columns.FirstOrDefault(_ => _.ColumnName.Equals(column.ColumnName, StringComparison.InvariantCultureIgnoreCase));
				Assert.IsNotNull(schemaColumn, column.ColumnName);

				if (column.CanBeNull)
					Assert.AreEqual(column.CanBeNull, schemaColumn.IsNullable, column.ColumnName + " Nullable");

				Assert.AreEqual(column.IsPrimaryKey, schemaColumn.IsPrimaryKey, column.ColumnName + " PrimaryKey");
			}

			//Assert.That(schemaTable.ForeignKeys.Count >= e.Associations.Count);
		}



		class PKTest
		{
			[PrimaryKey(1)]
			public int ID1;
			[PrimaryKey(2)]
			public int ID2;
		}

		[Test, DataContextSource(false)]
		public void DB2iSeriesTest(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var sp = conn.DataProvider.GetSchemaProvider();
				var dbSchema = sp.GetSchema(conn);
				var table = dbSchema.Tables.Single(t => t.TableName == "ALLTYPES");

				var col1 = table.Columns.Single(c => c.ColumnName == "BINARYDATATYPE");

				Assert.That(table.Columns.Single(c => c.ColumnName == "BINARYDATATYPE").ColumnType, Is.EqualTo("BINARY(20)"));
				Assert.That(table.Columns.Single(c => c.ColumnName == "VARCHARFORBITDATATYPE").ColumnType, Is.EqualTo("VARCHAR(5) FOR BIT DATA"));
			}
		}
	}
}
