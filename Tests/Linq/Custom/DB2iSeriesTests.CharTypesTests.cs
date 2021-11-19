using System;
using System.Collections.Generic;
using System.Linq;

using LinqToDB;
using LinqToDB.Mapping;

using NUnit.Framework;

namespace Tests.DataProvider
{
	[TestFixture]
	public partial class DB2iSeriesTests
	{
		[Table("ALLTYPES")]
		public class StringTestTable
		{
			[Column("ID")]
			public int Id;

			[Column("char20DataType")]
			public string? String;

			[Column("GRAPHICDATATYPE")]
			public string? NString;
		}

		[Table("ALLTYPES")]
		public class CharTestTable
		{
			[Column("ID")]
			public int Id;

			[Column("char20DataType")]
			public char? Char;

			[Column("GRAPHICDATATYPE")]
			public char? NChar;
		}

		// most of ending characters here trimmed by default by .net string TrimX methods
		// unicode test cases not used for String
		static readonly StringTestTable[] StringTestData =
		{
			new StringTestTable() { String = "test01",      NString = "test01"        },
			new StringTestTable() { String = "test02  ",    NString = "test02  "      },
			new StringTestTable() { String = "test03\x09 ", NString = "test03\x09 "   },
			new StringTestTable() { String = "test04\x0A ", NString = "test04\x0A "   },
			new StringTestTable() { String = "test05\x0B ", NString = "test05\x0B "   },
			new StringTestTable() { String = "test06\x0C ", NString = "test06\x0C "   },
			new StringTestTable() { String = "test07\x0D ", NString = "test07\x0D "   },
			new StringTestTable() { String = "test08\xA0 ", NString = "test08\xA0 "   },
			new StringTestTable() { String = "test09     ", NString = "test09\u2000 " },
			new StringTestTable() { String = "test10     ", NString = "test10\u2001 " },
			new StringTestTable() { String = "test11     ", NString = "test11\u2002 " },
			new StringTestTable() { String = "test12     ", NString = "test12\u2003 " },
			new StringTestTable() { String = "test13     ", NString = "test13\u2004 " },
			new StringTestTable() { String = "test14     ", NString = "test14\u2005 " },
			new StringTestTable() { String = "test15     ", NString = "test15\u2006 " },
			new StringTestTable() { String = "test16     ", NString = "test16\u2007 " },
			new StringTestTable() { String = "test17     ", NString = "test17\u2008 " },
			new StringTestTable() { String = "test18     ", NString = "test18\u2009 " },
			new StringTestTable() { String = "test19     ", NString = "test19\u200A " },
			new StringTestTable() { String = "test20     ", NString = "test20\u3000 " },
			new StringTestTable() { String = "test21\0   ", NString = "test21\0 "     },
			new StringTestTable()
		};

		static readonly CharTestTable[] CharTestData =
		{
			new CharTestTable() { Char = ' ',    NChar = ' '      },
			new CharTestTable() { Char = '\x09', NChar = '\x09'   },
			new CharTestTable() { Char = '\x0A', NChar = '\x0A'   },
			new CharTestTable() { Char = '\x0B', NChar = '\x0B'   },
			new CharTestTable() { Char = '\x0C', NChar = '\x0C'   },
			new CharTestTable() { Char = '\x0D', NChar = '\x0D'   },
			new CharTestTable() { Char = '\xA0', NChar = '\xA0'   },
			new CharTestTable() { Char = ' ',    NChar = '\u2000' },
			new CharTestTable() { Char = ' ',    NChar = '\u2001' },
			new CharTestTable() { Char = ' ',    NChar = '\u2002' },
			new CharTestTable() { Char = ' ',    NChar = '\u2003' },
			new CharTestTable() { Char = ' ',    NChar = '\u2004' },
			new CharTestTable() { Char = ' ',    NChar = '\u2005' },
			new CharTestTable() { Char = ' ',    NChar = '\u2006' },
			new CharTestTable() { Char = ' ',    NChar = '\u2007' },
			new CharTestTable() { Char = ' ',    NChar = '\u2008' },
			new CharTestTable() { Char = ' ',    NChar = '\u2009' },
			new CharTestTable() { Char = ' ',    NChar = '\u200A' },
			new CharTestTable() { Char = ' ',    NChar = '\u3000' },
			new CharTestTable() { Char = '\0',   NChar = '\0'     },
			new CharTestTable()
		};


		[Test]
		public void StringTrimming([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var lastId = db.GetTable<StringTestTable>().Select(_ => _.Id).Max();

				try
				{
					var testData = StringTestData.ToList();

					testData.ForEach(record =>
					{
						var query = db.GetTable<StringTestTable>().Value(_ => _.NString, record.NString);

						query = query.Value(_ => _.String, record.String);

						query.Insert();
					});

					var records = db.GetTable<StringTestTable>().Where(_ => _.Id > lastId).OrderBy(_ => _.Id).ToList();

					Assert.AreEqual(testData.Count, records.Count);

					testData.Zip(records, (expected, actual) => (expected, actual))
						.ToList().ForEach(x =>
					{
						var (expected, actual) = x;
						
						// DB2i ignores null characters
						Assert.AreEqual(expected.String?.TrimEnd(' ', '\0'), actual.String);

						if (TestProvNameDb2i.IsiSeriesOleDb(context) && expected.NString is { } && expected.NString.StartsWith("test20")) //OleDb strips \u3000
							Assert.AreEqual(expected.NString?.TrimEnd(), actual.NString);
						else
							Assert.AreEqual(expected.NString?.TrimEnd(' ', '\0'), actual.NString);
					});
				}
				finally
				{
					db.GetTable<StringTestTable>().Where(_ => _.Id > lastId).Delete();
				}
			}
		}

		[Test]
		public void CharTrimming([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var lastId = db.GetTable<CharTestTable>().Select(_ => _.Id).Max();

				try
				{
					//Strip null chars - not supported in DB2i
					var testData = CharTestData.Where(_ => _.NChar != '\0').ToList();

					testData.ForEach(record =>
					{
						var query = db.GetTable<CharTestTable>().Value(_ => _.NChar, record.NChar);

						query = query.Value(_ => _.Char, record.Char);

						query.Insert();
					});

					var records = db.GetTable<CharTestTable>().Where(_ => _.Id > lastId).OrderBy(_ => _.Id).ToList();
					
					Assert.AreEqual(testData.Count, records.Count);

					testData.Zip(records, (expected, actual) => (expected, actual))
						.ToList().ForEach(x =>
						{
							var (expected, actual) = x;

							Assert.AreEqual(TestProvNameDb2i.IsiSeriesOleDb(context) && expected.Char == ' '  ? '\0' : expected.Char, actual.Char);

							if (TestProvNameDb2i.IsiSeriesOleDb(context) && expected.NChar == '\u3000') // OleDb strips \u3000
								Assert.AreEqual('\0', actual.NChar);
							else
								Assert.AreEqual(expected.NChar == ' ' ? '\0' : expected.NChar, actual.NChar);
						});
				}
				finally
				{
					db.GetTable<CharTestTable>().Where(_ => _.Id > lastId).Delete();
				}
			}
		}

	}
}
