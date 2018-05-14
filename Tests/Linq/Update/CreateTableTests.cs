using System;
using System.Linq;
using System.Threading.Tasks;

using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;

using NUnit.Framework;

namespace Tests.xUpdate
{
	[TestFixture]
	public class CreateTableTests : TestBase
	{
		class TestTable
		{
			public int ID;
			public string Field1;
			public string Field2;
			public DateTime? CreatedOn;
		}

		[Test, DataContextSource(ProviderName.OracleNative)]
		public void CreateTable1(string context)
		{
			using (var db = GetDataContext(context))
			{
				db.MappingSchema.GetFluentMappingBuilder()
					.Entity<TestTable>()
						.Property(t => t.ID)
							.IsIdentity()
							.IsPrimaryKey()
						.Property(t => t.Field1)
							.HasLength(50);

				db.DropTable<TestTable>(throwExceptionIfNotExists: false);

				var table = db.CreateTable<TestTable>();
				var list = table.ToList();

				db.DropTable<TestTable>();
			}
		}

		[Test, DataContextSource(ProviderName.OracleNative)]
		public async Task CreateTable1Async(string context)
		{
			using (var db = GetDataContext(context))
			{
				db.MappingSchema.GetFluentMappingBuilder()
					.Entity<TestTable>()
						.Property(t => t.ID)
							.IsIdentity()
							.IsPrimaryKey()
						.Property(t => t.Field1)
							.HasLength(50);

				await db.DropTableAsync<TestTable>(throwExceptionIfNotExists: false);

				var table = await db.CreateTableAsync<TestTable>();
				var list = await table.ToListAsync();

				await db.DropTableAsync<TestTable>();
			}
		}

		enum FieldType1
		{
			[MapValue(1)] Value1,
			[MapValue(2)] Value2,
		}

		enum FieldType2
		{
			[MapValue("A")] Value1,
			[MapValue("AA")] Value2,
		}

		enum FieldType3 : short
		{
			Value1,
			Value2,
		}

		class TestEnumTable
		{
			public FieldType1 Field1;
			[Column(DataType = DataType.Int32)]
			public FieldType1? Field11;
			public FieldType2? Field2;
			[Column(DataType = DataType.Char, Length = 2)]
			public FieldType2 Field21;
			public FieldType3 Field3;
		}


		public enum jjj
		{
			aa,
			bb,
		}
		public class base_aa
		{
			public jjj dd { get; set; }
		}
		public class aa : base_aa
		{
			public int bb { get; set; }
			public string cc { get; set; }
		}

		public class qq
		{
			public int bb { get; set; }
			public string cc { get; set; }
		}

		[Test, DataContextSource]
		public void TestIssue160(string context)
		{
			using (var conn = GetDataContext(context))
			{
				conn.MappingSchema.GetFluentMappingBuilder()
					.Entity<aa>()
						.HasTableName("aa")
						.Property(t => t.bb).IsPrimaryKey()
						.Property(t => t.cc)
						.Property(t => t.dd).IsNotColumn()

					.Entity<qq>()
						.HasTableName("aa")
						.Property(t => t.bb).IsPrimaryKey()
						.Property(t => t.cc)
					;

				try
				{
					conn.CreateTable<qq>();
				}
				catch
				{
					conn.DropTable<qq>();
					conn.CreateTable<qq>();
				}

				conn.Insert(new aa
				{
					bb = 99,
					cc = "hallo",
					dd = jjj.aa
				});

				var qq = conn.GetTable<aa>().ToList().First();

				Assert.That(qq.bb, Is.EqualTo(99));
				Assert.That(qq.cc, Is.EqualTo("hallo"));

				conn.DropTable<qq>();
			}
		}
	}
}
