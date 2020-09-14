using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace Tests.SchemaProvider
{
	using LinqToDB;
	using LinqToDB.Data;
	using LinqToDB.DataProvider.SqlServer;
	using LinqToDB.Mapping;
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
					var tableName = schemaTable.CatalogName + "." +
						(schemaTable.IsDefaultSchema ? schemaTable.TableName : schemaTable.SchemaName + "." + schemaTable.TableName);

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

				Assert.That(dbSchema.Tables.Single(t => t.TableName.ToLower() == "doctor").ForeignKeys.Count, Is.EqualTo(1));
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
			[PrimaryKey(1)] public int ID1;
			[PrimaryKey(2)] public int ID2;
		}

		[Test, DataContextSource(false)]
		public void DB2Test(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var sp = conn.DataProvider.GetSchemaProvider();
				var dbSchema = sp.GetSchema(conn);
				var table = dbSchema.Tables.Single(t => t.TableName == "ALLTYPES");

				Assert.That(table.Columns.Single(c => c.ColumnName == "BINARYDATATYPE").ColumnType, Is.EqualTo("BINARY(20)"));
				Assert.That(table.Columns.Single(c => c.ColumnName == "VARBINARYDATATYPE").ColumnType, Is.EqualTo("VARBIN"));
			}
		}

		[Test]
		public void ToValidNameTest()
		{
			Assert.AreEqual("_1", SchemaProviderBase.ToValidName("1"));
			Assert.AreEqual("_1", SchemaProviderBase.ToValidName("    1   "));
			Assert.AreEqual("_1", SchemaProviderBase.ToValidName("\t1\t"));
		}


		[Test, DataContextSource(false, ProviderName.SQLiteMS
#if NETSTANDARD2_0
			, ProviderName.MySql, TestProvName.MySql57
#endif
			)]
		public void IncludeExcludeSchemaTest(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var exclude = conn.DataProvider.GetSchemaProvider()
					.GetSchema(conn, new GetSchemaOptions { ExcludedSchemas = new string[] { null } })
					.Tables.Select(_ => _.SchemaName)
					.Distinct()
					.ToList();
				exclude.Add(null);
				exclude.Add("");

				var schema1 = conn.DataProvider.GetSchemaProvider().GetSchema(conn, new GetSchemaOptions { ExcludedSchemas = exclude.ToArray() });
				var schema2 = conn.DataProvider.GetSchemaProvider().GetSchema(conn, new GetSchemaOptions { IncludedSchemas = new[] { "IncludeExcludeSchemaTest" } });

				Assert.IsEmpty(schema1.Tables);
				Assert.IsEmpty(schema2.Tables);
			}
		}

		[Test, DataContextSource(false, ProviderName.SQLiteMS
#if NETSTANDARD2_0
			, ProviderName.MySql, TestProvName.MySql57
#endif
			)]
		public void PrimaryForeignKeyTest(string context)
		{
			using (var db = new DataConnection(context))
			{
				var p = db.DataProvider.GetSchemaProvider();
				var s = p.GetSchema(db);

				var fkCountDoctor = s.Tables.Single(_ => _.TableName.Equals(nameof(Model.Doctor), StringComparison.OrdinalIgnoreCase)).ForeignKeys.Count;
				var pkCountDoctor = s.Tables.Single(_ => _.TableName.Equals(nameof(Model.Doctor), StringComparison.OrdinalIgnoreCase)).Columns.Count(_ => _.IsPrimaryKey);

				Assert.AreEqual(1, fkCountDoctor);
				Assert.AreEqual(1, pkCountDoctor);

				var fkCountPerson = s.Tables.Single(_ => _.TableName.Equals(nameof(Model.Person), StringComparison.OrdinalIgnoreCase) && !(_.SchemaName ?? "").Equals("MySchema", StringComparison.OrdinalIgnoreCase)).ForeignKeys.Count;
				var pkCountPerson = s.Tables.Single(_ => _.TableName.Equals(nameof(Model.Person), StringComparison.OrdinalIgnoreCase) && !(_.SchemaName ?? "").Equals("MySchema", StringComparison.OrdinalIgnoreCase)).Columns.Count(_ => _.IsPrimaryKey);

				Assert.AreEqual(2, fkCountPerson);
				Assert.AreEqual(1, pkCountPerson);
			}
		}

		//[Test]
		//public void SetForeignKeyMemberNameTest()
		//{
		//	var thisTable = new TableSchema { TableName = "Xxx", };
		//	var otherTable = new TableSchema { TableName = "Zzz", };

		//	var key = new ForeignKeySchema
		//	{
		//		KeyName = "FK_Xxx_YyyZzz",
		//		MemberName = "FK_Xxx_YyyZzz",
		//		ThisColumns = new List<ColumnSchema>
		//		{
		//			new ColumnSchema { MemberName = "XxxID", IsPrimaryKey = true },
		//			new ColumnSchema { MemberName = "YyyZzzID" },
		//		},
		//		OtherColumns = new List<ColumnSchema>
		//		{
		//			new ColumnSchema { MemberName = "ZzzID" },
		//		},
		//		ThisTable = thisTable,
		//		OtherTable = otherTable,
		//	};

		//	var key1 = new ForeignKeySchema
		//	{
		//		KeyName = "FK_Xxx_Zzz",
		//		MemberName = "FK_Xxx_Zzz",
		//		ThisColumns = new List<ColumnSchema>
		//		{
		//			new ColumnSchema { MemberName = "XxxID", IsPrimaryKey = true },
		//			new ColumnSchema { MemberName = "ZzzID" },
		//		},
		//		OtherColumns = new List<ColumnSchema>
		//		{
		//			new ColumnSchema { MemberName = "ZzzID" },
		//		},
		//		ThisTable = thisTable,
		//		OtherTable = otherTable,
		//	};

		//	key.ThisTable.ForeignKeys = new List<ForeignKeySchema> { key, key1 };
		//	key.ThisTable.Columns = key.ThisColumns;

		//	key.BackReference = new ForeignKeySchema
		//	{
		//		KeyName = key.KeyName + "_BackReference",
		//		MemberName = key.MemberName + "_BackReference",
		//		AssociationType = AssociationType.Auto,
		//		OtherTable = key.ThisTable,
		//		ThisColumns = key.OtherColumns,
		//		OtherColumns = key.ThisColumns,
		//	};

		//	SchemaProviderBase.SetForeignKeyMemberName(new GetSchemaOptions { }, key.ThisTable, key);

		//	Assert.That(key.MemberName, Is.EqualTo("YyyZzz"));
		//}
	}
}
