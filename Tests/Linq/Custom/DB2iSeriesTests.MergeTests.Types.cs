using System;
using System.Linq;

using LinqToDB;
using LinqToDB.Mapping;

using NUnit.Framework;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Table("unspecified")]
		class MergeTypes
		{
			[Column("Id")]
			[PrimaryKey]
			public int Id;

			[Column("Field1")]
			public int? FieldInt32;

			[Column("FieldInt64")]
			public long? FieldInt64;

			[Column("FieldBoolean")]
			public bool? FieldBoolean;

			[Column("FieldString")]
			public string? FieldString;

			[Column("FieldNString")]
			public string? FieldNString;

			[Column("FieldChar")]
			public char? FieldChar;

			[Column("FieldNChar")]
			public char? FieldNChar;

			[Column("FieldFloat")]
			public float? FieldFloat;

			[Column("FieldDouble")]
			public double? FieldDouble;

			[Column("FieldDateTime")]
			public DateTime? FieldDateTime;

			[Column("FieldBinary")]
			public byte[]? FieldBinary;

			[Column("FieldGuid")]
			public Guid? FieldGuid;

			[Column("FieldDecimal")]
			public decimal? FieldDecimal;

			[Column("FieldDate")]
			public DateTime? FieldDate;

			[Column("FieldTime")]
			public TimeSpan? FieldTime;

			[Column("FieldEnumString")]
			public StringEnum? FieldEnumString;

			[Column("FieldEnumNumber")]
			public NumberEnum? FieldEnumNumber;
		}

		public enum StringEnum
		{
			[MapValue("FIRST")]
			Value1,
			[MapValue("\b")]
			Value2,
			[MapValue("")]
			Value3,
			[MapValue(null)]
			Value4
		}

		public enum NumberEnum
		{
			[MapValue(int.MinValue + 1)]
			Value1,
			[MapValue(int.MaxValue)]
			Value2,
			[MapValue(0)]
			Value3,
			[MapValue(null)]
			Value4
		}

		private static ITable<MergeTypes> GetTypes1(IDataContext db)
		{
			return db.GetTable<MergeTypes>().TableName("TestMerge1");
		}

		private static ITable<MergeTypes> GetTypes2(IDataContext db)
		{
			return db.GetTable<MergeTypes>().TableName("TestMerge2");
		}

		private void PrepareTypesData(IDataContext db)
		{
			//using (new DisableLogging())
			{
				GetTypes1(db).Delete();
				GetTypes2(db).Delete();

				foreach (var record in InitialTypes1Data)
				{
					db.Insert(record, "TestMerge1");
				}

				foreach (var record in InitialTypes2Data)
				{
					db.Insert(record, "TestMerge2");
				}
			}
		}

		private static readonly MergeTypes[] InitialTypes1Data = new[]
		{
			new MergeTypes()
			{
				Id              = 1,
			},
			new MergeTypes()
			{
				Id              = 2,
				FieldInt32      = int.MinValue + 1,
				FieldInt64      = long.MinValue + 1,
				FieldBoolean    = true,
				FieldString     = "normal strinG",
				FieldNString    = "всЁ нормально",
				FieldChar       = '*',
				FieldNChar      = 'ё',
				FieldFloat      = -3.40282002E+38f, //float.MinValue,
				FieldDouble     = double.MinValue,
				FieldDateTime   = new DateTime(2000, 11, 12, 21, 14, 15, 167),
				FieldBinary     = new byte[0],
				FieldGuid       = Guid.Empty,
				FieldDecimal    = 12345678.9012345678M,
				FieldDate       = new DateTime(2000, 11, 23),
				FieldTime       = new TimeSpan(0, 9, 44, 33, 888).Add(TimeSpan.FromTicks(7654321)),
				FieldEnumString = StringEnum.Value1,
				FieldEnumNumber = NumberEnum.Value4
			},
			new MergeTypes()
			{
				Id              = 3,
				FieldInt32      = int.MaxValue,
				FieldInt64      = long.MaxValue,
				FieldBoolean    = false,
				FieldString     = "test\r\n\v\b\t\f",
				FieldNString    = "ЙЦУКЩывапрм\r\nq",
				FieldChar       = '&',
				FieldNChar      = '>',
				FieldFloat      = 3.40282002E+38f, //float.MaxValue,
				FieldDouble     = double.MaxValue,
				FieldDateTime   = new DateTime(2001, 10, 12, 21, 14, 15, 167),
				FieldBinary     = new byte[] { 0, 1, 2, 3, 0, 4 },
				FieldGuid       = new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
				FieldDecimal    = -99999999.9999999999M,
				FieldDate       = new DateTime(2123, 11, 23),
				FieldTime       = new TimeSpan(0, 0, 44, 33, 876).Add(TimeSpan.FromTicks(7654321)),
				FieldEnumString = StringEnum.Value2,
				FieldEnumNumber = NumberEnum.Value3
			},
			new MergeTypes()
			{
				Id              = 4,
				FieldInt32      = -123,
				FieldInt64      = 987,
				FieldBoolean    = null,
				FieldString     = "`~!@#$%^&*()_+{}|[]\\",
				FieldNString    = "<>?/.,;'щЩ\":",
				FieldChar       = '\r',
				FieldNChar      = '\n',
				FieldFloat      = 1.1755e-38f, //float.Epsilon,
				FieldDouble     = -2.2250738585072014e-308d, //-double.Epsilon,
				FieldDateTime   = new DateTime(2098, 10, 12, 21, 14, 15, 997),
				FieldBinary     = new byte[] { 255, 200, 100, 50, 20, 0 },
				FieldGuid       = new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"),
				FieldDecimal    = 99999999.9999999999M,
				FieldDate       = new DateTime(3210, 11, 23),
				FieldTime       = TimeSpan.Zero,
				FieldEnumString = StringEnum.Value3,
				FieldEnumNumber = NumberEnum.Value2
			}
		};

		private static readonly MergeTypes[] InitialTypes2Data = new[]
		{
			new MergeTypes()
			{
				Id              = 3,
				FieldInt32      = -123,
				FieldInt64      = 987,
				FieldBoolean    = null,
				FieldString     = "<>?/.,;'zZ\":",
				FieldNString    = "`~!@#$%^&*()_+{}|[]\\",
				FieldChar       = '\f',
				FieldNChar      = '\v',
				FieldFloat      = -1.1755e-38f, //-float.Epsilon,
				FieldDouble     = 2.2250738585072014e-308d, //double.Epsilon,
				FieldDateTime   = new DateTime(2098, 10, 12, 21, 14, 15, 907),
				FieldBinary     = new byte[] { 255, 200, 100, 50, 20, 0 },
				FieldGuid       = new Guid("ffffffff-ffff-ffff-FFFF-ffffffffffff"),
				FieldDecimal    = -0.123M,
				FieldDate       = new DateTime(3210, 11, 23),
				FieldTime       = TimeSpan.FromHours(24).Add(TimeSpan.FromTicks(-1)),
				FieldEnumString = StringEnum.Value4,
				FieldEnumNumber = NumberEnum.Value1
			},
			new MergeTypes()
			{
				Id              = 4,
				FieldInt32      = int.MaxValue,
				FieldInt64      = long.MaxValue,
				FieldBoolean    = false,
				FieldString     = "test\r\n\v\b\t",
				FieldNString    = "ЙЦУКЩывапрм\r\nq",
				FieldChar       = '1',
				FieldNChar      = ' ',
				FieldFloat      = 3.40282002E+38f, //float.MaxValue,
				FieldDouble     = double.MaxValue,
				FieldDateTime   = new DateTime(2001, 10, 12, 21, 14, 15, 167),
				FieldBinary     = new byte[] { 0, 1, 2, 3, 0, 4 },
				FieldGuid       = new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
				FieldDecimal    = -99999999.9999999999M,
				FieldDate       = new DateTime(2123, 11, 23),
				FieldTime       = new TimeSpan(0, 14, 44, 33, 234),
				FieldEnumString = StringEnum.Value2,
				FieldEnumNumber = NumberEnum.Value3
			},
			new MergeTypes()
			{
				Id              = 5,
				FieldInt32      = -123,
				FieldInt64      = 987,
				FieldBoolean    = null,
				FieldString     = "<>?/.,;'zZ\":",
				FieldNString    = "`~!@#$%^&*()_+{}|[]\\",
				FieldChar       = ' ',
				FieldNChar      = ' ',
				FieldFloat      = -1.1755e-38f, //-float.Epsilon,
				FieldDouble     = 2.2250738585072014e-308d, //double.Epsilon,
				FieldDateTime   = new DateTime(2098, 10, 12, 21, 14, 15, 913),
				FieldBinary     = new byte[] { 255, 200, 100, 50, 20, 0 },
				FieldGuid       = new Guid("ffffffff-ffff-ffff-FFFF-ffffffffffff"),
				FieldDecimal    = -0.123M,
				FieldDate       = new DateTime(3210, 11, 23),
				FieldTime       = TimeSpan.FromHours(24).Add(TimeSpan.FromTicks(-1)),
				FieldEnumString = StringEnum.Value4,
				FieldEnumNumber = NumberEnum.Value1
			},
			new MergeTypes()
			{
				Id              = 6,
				FieldInt32      = int.MaxValue,
				FieldInt64      = long.MaxValue,
				FieldBoolean    = false,
				FieldString     = "test\r\n\v\b\t \r ",
				FieldNString    = "ЙЦУКЩывапрм\r\nq \r ",
				FieldChar       = '-',
				FieldNChar      = '~',
				FieldFloat      = 3.40282002E+38f, //float.MaxValue,
				FieldDouble     = double.MaxValue,
				FieldDateTime   = new DateTime(2001, 10, 12, 21, 14, 15, 167),
				FieldBinary     = new byte[] { 0, 1, 2, 3, 0, 4 },
				FieldGuid       = new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
				FieldDecimal    = -99999999.9999999999M,
				FieldDate       = new DateTime(2123, 11, 23),
				FieldTime       = new TimeSpan(0, 22, 44, 33, 0),
				FieldEnumString = StringEnum.Value2,
				FieldEnumNumber = NumberEnum.Value3
			}
		};

		
		[Test]
		public void TestMergeTypes([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = GetDataContext(context))
			{
				PrepareTypesData(db);

				var result1 = GetTypes1(db).OrderBy(_ => _.Id).ToList();
				var result2 = GetTypes2(db).OrderBy(_ => _.Id).ToList();

				Assert.AreEqual(InitialTypes1Data.Length, result1.Count);
				Assert.AreEqual(InitialTypes2Data.Length, result2.Count);

				var provider = GetProviderName(context, out var _);
				for (var i = 0; i < InitialTypes1Data.Length; i++)
				{
					AssertTypesRow(InitialTypes1Data[i], result1[i], provider);
				}

				for (var i = 0; i < InitialTypes2Data.Length; i++)
				{
					AssertTypesRow(InitialTypes2Data[i], result2[i], provider);
				}
			}
		}

		private void AssertTypesRow(MergeTypes expected, MergeTypes actual, string provider)
		{
			Assert.AreEqual(expected.Id, actual.Id);
			Assert.AreEqual(expected.FieldInt32, actual.FieldInt32);
			Assert.AreEqual(expected.FieldInt64, actual.FieldInt64);
			Assert.AreEqual(expected.FieldBoolean, actual.FieldBoolean);
			AssertString(expected.FieldString, actual.FieldString, provider);
			AssertString(expected.FieldNString, actual.FieldNString, provider);
			AssertChar(expected.FieldChar, actual.FieldChar, provider);
			AssertChar(expected.FieldChar, actual.FieldChar, provider);
			Assert.AreEqual(expected.FieldFloat, actual.FieldFloat);
			Assert.AreEqual(expected.FieldDouble, actual.FieldDouble);
			Assert.AreEqual(expected.FieldDateTime, actual.FieldDateTime);
			Assert.AreEqual(expected.FieldBinary, actual.FieldBinary);
			Assert.AreEqual(expected.FieldGuid, actual.FieldGuid);
			Assert.AreEqual(expected.FieldDecimal, actual.FieldDecimal);
			Assert.AreEqual(expected.FieldDate, actual.FieldDate);
			AssertTime(expected.FieldTime, actual.FieldTime, provider);

			if (expected.FieldEnumString == StringEnum.Value4)
				Assert.IsNull(actual.FieldEnumString);
			else
				Assert.AreEqual(expected.FieldEnumString, actual.FieldEnumString);

			if (expected.FieldEnumNumber == NumberEnum.Value4)
				Assert.IsNull(actual.FieldEnumNumber);
			else
				Assert.AreEqual(expected.FieldEnumNumber, actual.FieldEnumNumber);
		}

		private static void AssertChar(char? expected, char? actual, string provider)
		{
			if (TestProvNameDb2i.IsiSeriesOleDb(provider) && expected == ' ')
				expected = '\0';
			
			Assert.AreEqual(expected, actual);
		}

		private static void AssertString(string? expected, string? actual, string provider)
		{
			if (TestProvNameDb2i.IsiSeriesOleDb(provider))
				expected = expected?.TrimEnd(' ');

			Assert.AreEqual(expected, actual);
		}

		private static void AssertTime(TimeSpan? expected, TimeSpan? actual, string provider)
		{
			if (expected != null)
				expected = TimeSpan.FromTicks((expected.Value.Ticks / 10000000) * 10000000);
			
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void TestTypesInsertByMerge([Tests.xUpdate.MergeTests.MergeDataContextSource()]
			string context)
		{
			using (var db = GetDataContext(context))
			{
				using (new DisableLogging())
				{
					GetTypes1(db).Delete();
					GetTypes2(db).Delete();
				}

				GetTypes1(db).Merge().Using(InitialTypes1Data).OnTargetKey().InsertWhenNotMatched().Merge();
				GetTypes2(db).Merge().Using(InitialTypes2Data).OnTargetKey().InsertWhenNotMatched().Merge();

				var result1 = GetTypes1(db).OrderBy(_ => _.Id).ToList();
				var result2 = GetTypes2(db).OrderBy(_ => _.Id).ToList();

				Assert.AreEqual(InitialTypes1Data.Length, result1.Count);
				Assert.AreEqual(InitialTypes2Data.Length, result2.Count);

				var provider = GetProviderName(context, out var _);
				for (var i = 0; i < InitialTypes1Data.Length; i++)
				{
					AssertTypesRow(InitialTypes1Data[i], result1[i], provider);
				}

				for (var i = 0; i < InitialTypes2Data.Length; i++)
				{
					AssertTypesRow(InitialTypes2Data[i], result2[i], provider);
				}
			}
		}

		[Test]
		public void TestDB2NullsInSource([xUpdate.MergeTests.MergeDataContextSource()] string context)
		{
			using (var db = GetDataContext(context))
			{
				db.GetTable<MergeTypes>()
					.TableName("TestMerge1")
					.Merge()
					.Using(new[] { new MergeTypes() { Id = 1 }, new MergeTypes() { Id = 2 } })
					.OnTargetKey()
					.InsertWhenNotMatched()
					.Merge();
			}
		}
	}
}
