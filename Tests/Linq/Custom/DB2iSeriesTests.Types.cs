using System;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Data;
using NUnit.Framework;
using LinqToDB.Mapping;
using LinqToDB.Tools.Comparers;

#if NETFRAMEWORK
using IBM.Data.DB2.iSeries;
#endif

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void TestParameters([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.ExecuteScalarParameterObject<string>("p", "varchar(10)", new { p = 1 }),
					Is.EqualTo("1"));
				Assert.That(conn.ExecuteScalarParameterObject<string>("p", "varchar(10)", new { p = "1" }),
					Is.EqualTo("1"));
				Assert.That(conn.ExecuteScalarParameter<string>("p", "varchar(10)", 1),
					Is.EqualTo("1"));
				Assert.That(conn.ExecuteScalarParameter<string>("p", "varchar(10)", "1"),
					Is.EqualTo("1"));
				Assert.That(
					conn.ExecuteScalarParameterObject<int>($"cast({conn.GetParameterMarker("p1")} as int) - cast({conn.GetParameterMarker("p2")} as int)", new { p1 = 3, p2 = 2 }),
					Is.EqualTo(1));
				Assert.That(
					conn.ExecuteScalarParameterObject<int>($"cast({conn.GetParameterMarker("p2")} as int) - cast({conn.GetParameterMarker("p1")} as int)", new { p2 = 3, p1 = 2 }),
					Is.EqualTo(1));

				//This fails on ODBC as parameters are not named and should be set in order
				//Assert.That(
				//	conn.ExecuteScalarParameterObject<int>($"cast({conn.GetParameterName("p2")} as int) - cast({conn.GetParameterName("p1")} as int)", new { p1 = 3, p2 = 2 }),
				//	Is.EqualTo(-1));

				//This fails on ODBC as casting int parameter to nvarchar produces null
				//Assert.That(conn.ExecuteScalarParameter<string>("p", "nvarchar(10)", 1),
				//	Is.EqualTo("1"));
			}
		}

		[Test]
		//DecFloatTests break on AccessClient with cultures that have a different decimal point than period.
		[SetCulture("en-US")]
		public void TestDataTypes([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(TestType<decimal?>(conn, "decfloat16DataType", DataType.Decimal, castTo: "DECIMAL(20, 10)"), Is.EqualTo(888.456m));
				Assert.That(TestType<decimal?>(conn, "decfloat34DataType", DataType.Decimal, castTo: "DECIMAL(20, 10)"), Is.EqualTo(777.987m));

				Assert.That(TestType<long?>(conn, "bigintDataType", DataType.Int64), Is.EqualTo(1000000L));
				Assert.That(TestType<int?>(conn, "intDataType", DataType.Int32), Is.EqualTo(444444));
				Assert.That(TestType<short?>(conn, "smallintDataType", DataType.Int16), Is.EqualTo(100));
				Assert.That(TestType<decimal?>(conn, "decimalDataType", DataType.Decimal), Is.EqualTo(666m));

				Assert.That(TestType<float?>(conn, "realDataType", DataType.Single), Is.EqualTo(222.987f));
				Assert.That(TestType<double?>(conn, "doubleDataType", DataType.Double), Is.EqualTo(555.987d));

				Assert.That(TestType<string>(conn, "charDataType", DataType.Char), Is.EqualTo("Y"));
				Assert.That(TestType<string>(conn, "charDataType", DataType.NChar), Is.EqualTo("Y"));

				Assert.That(TestType<string>(conn, "varcharDataType", DataType.VarChar), Is.EqualTo("var-char"));
				Assert.That(TestType<string>(conn, "varcharDataType", DataType.NVarChar), Is.EqualTo("var-char"));
				Assert.That(TestType<string>(conn, "clobDataType", DataType.Text), Is.EqualTo("567"));
				Assert.That(TestType<string>(conn, "dbclobDataType", DataType.NText), Is.EqualTo("890"));

				Assert.That(TestType<byte[]>(conn, "binaryDataType", DataType.Binary),
					Is.EqualTo(new byte[] { 0xF1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));
				Assert.That(TestType<byte[]>(conn, "varbinaryDataType", DataType.VarBinary), Is.EqualTo(new byte[] { 0xF4 }));
				Assert.That(
					TestType<byte[]>(conn, "blobDataType", DataType.Blob, skipDefaultNull: true, skipUndefinedNull: true,
						skipDefault: true, skipUndefined: true), Is.EqualTo(new byte[] { 0xF2, 0xF3, 0xF4 }));
				Assert.That(
					TestType<byte[]>(conn, "blobDataType", DataType.VarBinary, skipDefaultNull: true, skipUndefinedNull: true,
						skipDefault: true, skipUndefined: true), Is.EqualTo(new byte[] { 0xF2, 0xF3, 0xF4 }));

				Assert.That(TestType<string>(conn, "graphicDataType", DataType.Char), Is.EqualTo("graphic"));
				Assert.That(TestType<string>(conn, "vargraphicDataType", DataType.VarChar), Is.EqualTo("vargraphic"));

				Assert.That(TestType<DateTime?>(conn, "dateDataType", DataType.Date, skipDefault: true, skipUndefined: true),
					Is.EqualTo(new DateTime(2012, 12, 12)));

				Assert.That(TestType<TimeSpan?>(conn, "timeDataType", DataType.Time), Is.EqualTo(new TimeSpan(12, 12, 12)));

				Assert.That(
					TestType<DateTime?>(conn, "timestampDataType", DataType.DateTime2, skipDefault: true, skipUndefined: true),
					Is.EqualTo(new DateTime(2012, 12, 12, 12, 12, 12, 0)));

				Assert.That(conn.Execute<byte[]>("SELECT rowidDataType FROM AllTypes WHERE ID = 2").Length, Is.Not.EqualTo(0));

				//XML not supported in ODBC driver
				if (!TestProvNameDb2i.IsiSeriesODBC(context))
				{
					Assert.That(TestType<string>(conn, "xmlDataType", DataType.Xml, skipPass: true),
						Is.EqualTo("<root><element strattr=\"strvalue\" intattr=\"12345\"/></root>"));
				}
			}
		}

#if NETFRAMEWORK
		[Test]
		//DecFloatTests break on AccessClient with cultures that have a different decimal point than period.
		[SetCulture("en-US")]
		public void TestDataTypes_AccessClient([IncludeDataSources(TestProvNameDb2i.All_AccessClient)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(TestType<iDB2DecFloat16?>(conn, "decfloat16DataType", DataType.Decimal), Is.EqualTo(new iDB2DecFloat16(888.456m)));
				Assert.That(TestType<iDB2DecFloat34?>(conn, "decfloat34DataType", DataType.Decimal).ToString(), Is.EqualTo(new iDB2DecFloat34(777.987m).ToString()));
				
				Assert.That(TestType<iDB2BigInt?>(conn, "bigintDataType", DataType.Int64), Is.EqualTo(new iDB2BigInt(1000000L)));
				Assert.That(TestType<iDB2Integer?>(conn, "intDataType", DataType.Int32), Is.EqualTo(new iDB2Integer(444444)));
				Assert.That(TestType<iDB2SmallInt?>(conn, "smallintDataType", DataType.Int16), Is.EqualTo(new iDB2SmallInt(100)));
				Assert.That(TestType<iDB2Real?>(conn, "realDataType", DataType.Single), Is.EqualTo(new iDB2Real(222.987f)));
				Assert.That(TestType<iDB2Double?>(conn, "doubleDataType", DataType.Double), Is.EqualTo(new iDB2Double(555.987d)));
				Assert.That(TestType<iDB2VarChar?>(conn, "charDataType", DataType.Char), Is.EqualTo(new iDB2VarChar("Y")));
				Assert.That(TestType<iDB2Date?>(conn, "dateDataType", DataType.Date), Is.EqualTo(new iDB2Date(new DateTime(2012, 12, 12))));
				Assert.That(TestType<iDB2Time?>(conn, "timeDataType", DataType.Time), Is.EqualTo(new iDB2Time(new DateTime(1, 1, 1, 12, 12, 12))));
				Assert.That(TestType<iDB2TimeStamp?>(conn, "timestampDataType", DataType.DateTime2), Is.EqualTo(new iDB2TimeStamp(new DateTime(2012, 12, 12, 12, 12, 12, 0))));
				Assert.That(conn.Execute<iDB2Rowid>("SELECT rowidDataType FROM AllTypes WHERE ID = 2").Value.Length, Is.Not.EqualTo(0));

				TestType<iDB2Clob>(conn, "clobDataType", DataType.Text, skipNotNull: true);
				TestType<iDB2Blob>(conn, "blobDataType", DataType.VarBinary, skipNotNull: true);
				TestType<iDB2Xml>(conn, "xmlDataType", DataType.Xml, skipPass: true);

				Assert.That(TestType<iDB2Decimal?>(conn, "decimalDataType", DataType.Decimal).ToString(), Is.EqualTo(new iDB2Decimal(666m).ToString()));
				Assert.That(TestType<iDB2Binary>(conn, "varbinaryDataType", DataType.VarBinary).ToString(), Is.EqualTo(new iDB2Binary(new byte[] { 0xF4 }).ToString()));
			}
		}
#endif

		[Test]
		//Test uses string format to build sql values, invariant culture is needed
		[SetCulture("en-US")]
		public void TestNumerics([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			var skipDecFloat = TestProvNameDb2i.IsiSeriesOleDb(context) ? " decfloat" : "";
			
			using (var conn = new DataConnection(context))
			{
				TestSimple<decimal>(conn, 1m, DataType.VarNumeric);

				TestSimple<sbyte>(conn, 1, DataType.SByte);
				TestSimple<short>(conn, 1, DataType.Int16);
				TestSimple<int>(conn, 1, DataType.Int32);
				TestSimple<long>(conn, 1L, DataType.Int64);
				TestSimple<byte>(conn, 1, DataType.Byte);
				TestSimple<ushort>(conn, 1, DataType.UInt16);
				TestSimple<uint>(conn, 1u, DataType.UInt32);
				TestSimple<ulong>(conn, 1ul, DataType.UInt64);
				TestSimple<float>(conn, 1, DataType.Single);
				TestSimple<double>(conn, 1d, DataType.Double);
				TestSimple<decimal>(conn, 1m, DataType.Decimal);
				TestSimple<decimal>(conn, 1m, DataType.Money);
				TestSimple<decimal>(conn, 1m, DataType.SmallMoney);
				//OleDb default mapping for VarNumeric is not supported on this provider
				if (!TestProvNameDb2i.IsiSeriesOleDb(context))
					TestSimple<decimal>(conn, 1m, DataType.VarNumeric);

				TestNumeric(conn, sbyte.MinValue, DataType.SByte);
				TestNumeric(conn, sbyte.MaxValue, DataType.SByte);
				TestNumeric(conn, short.MinValue, DataType.Int16);
				TestNumeric(conn, short.MaxValue, DataType.Int16);
				TestNumeric(conn, int.MinValue, DataType.Int32, "smallint");
				TestNumeric(conn, int.MaxValue, DataType.Int32, "smallint real");
				TestNumeric(conn, long.MinValue, DataType.Int64, "smallint int double" + skipDecFloat);
				TestNumeric(conn, long.MaxValue, DataType.Int64, "smallint int double real" + skipDecFloat);
				TestNumeric(conn, byte.MaxValue, DataType.Byte);
				TestNumeric(conn, ushort.MaxValue, DataType.UInt16, "smallint");
				TestNumeric(conn, uint.MaxValue, DataType.UInt32, "smallint int real");
				TestNumeric(conn, ulong.MaxValue, DataType.UInt64, "smallint int real bigint double" + skipDecFloat);
				TestNumeric(conn, -3.40282306E+38f, DataType.Single, "bigint int smallint decimal(31) decfloat");
				TestNumeric(conn, 3.40282306E+38f, DataType.Single, "bigint int smallint decimal(31) decfloat");
				TestNumeric(conn, -1.79E+308d, DataType.Double, "bigint int smallint decimal(31) decfloat real");
				TestNumeric(conn, 1.79E+308d, DataType.Double, "bigint int smallint decimal(31) decfloat real");
				TestNumeric(conn, decimal.MinValue, DataType.Decimal, "bigint int smallint double real" + skipDecFloat);
				TestNumeric(conn, decimal.MaxValue, DataType.Decimal, "bigint int smallint double real" + skipDecFloat);
				TestNumeric(conn, decimal.MinValue, DataType.VarNumeric, "bigint int smallint double real" + skipDecFloat);
				TestNumeric(conn, decimal.MaxValue, DataType.VarNumeric, "bigint int smallint double real" + skipDecFloat);
				TestNumeric(conn, -922337203685477m, DataType.Money, "int smallint real");
				TestNumeric(conn, +922337203685477m, DataType.Money, "int smallint real");
				TestNumeric(conn, -214748m, DataType.SmallMoney, "smallint");
				TestNumeric(conn, +214748m, DataType.SmallMoney, "smallint");
			}
		}

		[Test]
		public void TestDate([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var dateTime = new DateTime(2012, 12, 12);

				Assert.That(
					conn.ExecuteScalar<DateTime>("'2012-12-12'", "date"), Is.EqualTo(dateTime));
				Assert.That(
					conn.ExecuteScalar<DateTime?>("'2012-12-12'", "date"), Is.EqualTo(dateTime));
				Assert.That(
					conn.ExecuteScalarParameter<DateTime>(DataParameter.Date("p", dateTime), "date"), Is.EqualTo(dateTime));
				Assert.That(
					conn.ExecuteScalarParameter<DateTime>("p", "date", dateTime, DataType.Date), Is.EqualTo(dateTime));

				//iSeries native provider and oledb provider cannot assign datetime parameter to date
				if (!TestProvNameDb2i.IsiSeriesAccessClient(context) 
					&& !TestProvNameDb2i.IsiSeriesOleDb(context))
				{
					Assert.That(
					conn.ExecuteScalarParameter<DateTime>("p", "date", dateTime), Is.EqualTo(dateTime));
				}
			}
		}

		[Test]
		public void TestDateTime([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var dateTime = new DateTime(2012, 12, 12, 12, 12, 12);

				Assert.That(
					conn.ExecuteScalar<DateTime>("'2012-12-12 12:12:12'", "timestamp"), Is.EqualTo(dateTime));
				Assert.That(
					conn.ExecuteScalar<DateTime?>("'2012-12-12 12:12:12'", "timestamp"), Is.EqualTo(dateTime));

				Assert.That(
					conn.ExecuteScalarParameter<DateTime>("p", "timestamp", dateTime), Is.EqualTo(dateTime));
				Assert.That(
					conn.ExecuteScalarParameter<DateTime>(DataParameter.DateTime("p", dateTime), "timestamp"), Is.EqualTo(dateTime));
				Assert.That(
					conn.ExecuteScalarParameter<DateTime?>("p", "timestamp", dateTime), Is.EqualTo(dateTime));
				Assert.That(
					conn.ExecuteScalarParameter<DateTime?>("p", "timestamp", dateTime, DataType.DateTime), Is.EqualTo(dateTime));
			}
		}

		[Test]
		public void TestTimeSpan([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var time = new TimeSpan(12, 12, 12);
				var dateTime = new DateTime(2012, 12, 12, 12, 12, 12);
				var dateTimeOffset = new DateTimeOffset(2012, 12, 12, 12, 12, 12, TimeSpan.Zero);

				Assert.That(
					conn.ExecuteScalar<TimeSpan>("'12:12:12'", "time"), Is.EqualTo(time));
				Assert.That(
					conn.ExecuteScalar<TimeSpan?>("'12:12:12'", "time"), Is.EqualTo(time));

				Assert.That(
					conn.ExecuteScalarParameter<TimeSpan>("p", "time", time), Is.EqualTo(time));
				Assert.That(
					conn.ExecuteScalarParameter<TimeSpan>(DataParameter.Time("p", time), "time"), Is.EqualTo(time));
				Assert.That(
					conn.ExecuteScalarParameter<TimeSpan?>("p", "time", time), Is.EqualTo(time));
				Assert.That(
					conn.ExecuteScalarParameter<TimeSpan?>("p", "time", time, DataType.Time), Is.EqualTo(time));
				Assert.That(
					conn.ExecuteScalarParameter<TimeSpan?>("p", "time", dateTime, DataType.Time), Is.EqualTo(time));
				Assert.That(
					conn.ExecuteScalarParameter<TimeSpan>("p", "time", dateTimeOffset, DataType.Time), Is.EqualTo(time));
			}
		}

		[Test]
		public void TestChar([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			var asciiChar = '1'; var quotedAsciiChar = asciiChar.ToString().AsQuoted();
			var unicodeChar = 'α'; var quotedUnicodeChar = unicodeChar.ToString().AsQuoted();

			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.ExecuteScalar<char>(quotedAsciiChar, "char"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedAsciiChar, "char"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalar<char>(quotedAsciiChar, "char(1)"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedAsciiChar, "char(1)"), Is.EqualTo(asciiChar));

				Assert.That(conn.ExecuteScalar<char>(quotedAsciiChar, "varchar(1)"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedAsciiChar, "varchar(1)"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalar<char>(quotedAsciiChar, "varchar(20)"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedAsciiChar, "varchar(20)"), Is.EqualTo(asciiChar));

				Assert.That(conn.ExecuteScalar<char>(quotedUnicodeChar, "nchar"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedUnicodeChar, "nchar"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalar<char>(quotedUnicodeChar, "nchar(1)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedUnicodeChar, "nchar(1)"), Is.EqualTo(unicodeChar));

				Assert.That(conn.ExecuteScalar<char>(quotedUnicodeChar, "nvarchar(1)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedUnicodeChar, "nvarchar(1)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalar<char>(quotedUnicodeChar, "nvarchar(20)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalar<char?>(quotedUnicodeChar, "nvarchar(20)"), Is.EqualTo(unicodeChar));

				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.Char("p", asciiChar), "char"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.Char("p", asciiChar), "char"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.Char("p", asciiChar), "char(1)"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.Char("p", asciiChar), "char(1)"), Is.EqualTo(asciiChar));

				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.Char("p", asciiChar), "char"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.Char("p", asciiChar), "char"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.Char("p", asciiChar), "char(1)"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.Char("p", asciiChar), "char(1)"), Is.EqualTo(asciiChar));

				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.VarChar("p", asciiChar), "varchar(1)"), Is.EqualTo(asciiChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.VarChar("p", asciiChar), "varchar(1)"), Is.EqualTo(asciiChar));

				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.NChar("p", unicodeChar), "nchar(1)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.NChar("p", unicodeChar), "nchar(1)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.NVarChar("p", unicodeChar), "nvarchar(1)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.NVarChar("p", unicodeChar), "nvarchar(1)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalarParameter<char>(DataParameter.Create("p", unicodeChar), "nvarchar(10)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(DataParameter.Create("p", unicodeChar), "nvarchar(10)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalarParameter<char>(new DataParameter { Name = "p", Value = unicodeChar }, "nvarchar(10)"), Is.EqualTo(unicodeChar));
				Assert.That(conn.ExecuteScalarParameter<char?>(new DataParameter { Name = "p", Value = unicodeChar }, "nvarchar(10)"), Is.EqualTo(unicodeChar));
			}
		}

		[Test]
		public void TestString([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			string ExecuteScalarTest(DataConnection conn, string value, string castTo = null)
			{
				return  conn.ExecuteScalar<string>(value, castTo);
			}

			using (var conn = new DataConnection(context))
			{
				var asciiText = "123ab"; var quotedAsciiText = asciiText.AsQuoted();
				var unicodeText = "αβγδε"; var quotedUnicodeText = unicodeText.AsQuoted();

				Assert.That(ExecuteScalarTest(conn, quotedAsciiText, "char(5)"), Is.EqualTo(asciiText));
				Assert.That(ExecuteScalarTest(conn, quotedAsciiText, "char(20)"), Is.EqualTo(asciiText));
				Assert.That(ExecuteScalarTest(conn, "NULL", "char(5)"), Is.Null);

				Assert.That(ExecuteScalarTest(conn, quotedAsciiText, "varchar(5)"), Is.EqualTo(asciiText));
				Assert.That(ExecuteScalarTest(conn, quotedAsciiText, "varchar(20)"), Is.EqualTo(asciiText));
				Assert.That(ExecuteScalarTest(conn, "NULL", "varchar(5)"), Is.Null);

				Assert.That(ExecuteScalarTest(conn, quotedAsciiText, "clob"), Is.EqualTo(asciiText));
				Assert.That(ExecuteScalarTest(conn, "NULL", "clob"), Is.Null);

				Assert.That(ExecuteScalarTest(conn, quotedUnicodeText, "nchar(5)"), Is.EqualTo(unicodeText));
				//Assert.That(ExecuteScalarTest(conn, quotedUnicodeText, "nchar(20)"), Is.EqualTo(unicodeText));
				Assert.That(ExecuteScalarTest(conn, "NULL", "nchar(5)"), Is.Null);

				Assert.That(ExecuteScalarTest(conn, quotedUnicodeText, "nvarchar(5)"), Is.EqualTo(unicodeText));
				Assert.That(ExecuteScalarTest(conn, quotedUnicodeText, "nvarchar(20)"), Is.EqualTo(unicodeText));
				Assert.That(ExecuteScalarTest(conn, "NULL", "nvarchar(5)"), Is.Null);

				Assert.That(ExecuteScalarTest(conn, quotedUnicodeText, "nclob"), Is.EqualTo(unicodeText));
				Assert.That(ExecuteScalarTest(conn, "NULL", "nclob"), Is.Null);

				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.Char("p", asciiText), "char(5)"),
					Is.EqualTo(asciiText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.VarChar("p", asciiText), "varchar(10)"),
					Is.EqualTo(asciiText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.Text("p", asciiText), "varchar(10)"),
					Is.EqualTo(asciiText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.Text("p", asciiText), "clob(10)"),
					Is.EqualTo(asciiText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.Create("p", asciiText), "clob"),
					Is.EqualTo(asciiText));

				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.NChar("p", unicodeText), "nchar(5)"),
					Is.EqualTo(unicodeText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.NVarChar("p", unicodeText), "nvarchar(10)"),
					Is.EqualTo(unicodeText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.NText("p", unicodeText), "nvarchar(10)"),
					Is.EqualTo(unicodeText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.Create("p", unicodeText), "nclob"),
					Is.EqualTo(unicodeText));
				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.NText("p", unicodeText), "nclob(10)"),
					Is.EqualTo(unicodeText));

				Assert.That(
					conn.ExecuteScalarParameter<string>(DataParameter.Create("p", (string)null), "nvarchar(10)"),
					Is.Null);

				//This case fails on ODBC provider. Casting a numeric parameter to nvarchar/nclob produces null
				//Assert.That(
				//	conn.ExecuteScalarParameter<string>("p", "nvarchar(10)", 1),
				//	Is.EqualTo("1"));
			}
		}

		[Test]
		public void TestBinary([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			// results are going to be bytes from EDCIDC character set
			var arr1 = new byte[] { 241, 242 };
			var arr2 = new byte[] { 241, 242, 243, 244 };

			using (var conn = new DataConnection(context))
			{
				Assert.That(
					conn.ExecuteScalar<byte[]>("'12'", "char(2) for bit data"),
					Is.EqualTo(arr1));
				Assert.That(
					conn.ExecuteScalar<Binary>("'1234'", "char(4) for bit data"),
					Is.EqualTo(new Binary(arr2)));

				Assert.That(
					conn.ExecuteScalar<byte[]>("'12'", "varchar(2) for bit data"),
					Is.EqualTo(arr1));
				Assert.That(
					conn.ExecuteScalar<Binary>("'1234'", "varchar(4) for bit data"),
					Is.EqualTo(new Binary(arr2)));
			}
		}

		[Test]
		public void TestGuidAsBinary([IncludeDataSources(TestProvNameDb2i.All_NonGAS)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var guid = new Guid();

				Assert.That(
					conn.ExecuteScalar<Guid>($"'{guid}'", "varchar(38)"), Is.EqualTo(guid));

				Assert.That(
					conn.ExecuteScalar<Guid?>($"'{guid}'", "varchar(38)"), Is.EqualTo(guid));

				Assert.That(
					conn.ExecuteScalarParameter<Guid>(DataParameter.Create("p", guid), "char(16) for bit data"),
					Is.EqualTo(guid));
				Assert.That(
					conn.ExecuteScalarParameter<Guid>("p", "char(16) for bit data", guid),
					Is.EqualTo(guid));
			}
		}

		[Test]
		public void TestGuidAsString([IncludeDataSources(TestProvNameDb2i.All_GAS)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var guid = new Guid();

				Assert.That(
					conn.ExecuteScalar<Guid>($"'{guid}'", "varchar(38)"), Is.EqualTo(guid));

				Assert.That(
					conn.ExecuteScalar<Guid?>($"'{guid}'", "varchar(38)"), Is.EqualTo(guid));
			}
		}

		[Test]
		public void TestXml([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.ExecuteScalar<string>("'<xml/>'"), Is.EqualTo("<xml/>"));
				Assert.That(conn.ExecuteScalar<XDocument>("'<xml/>'").ToString(), Is.EqualTo("<xml />"));
				Assert.That(conn.ExecuteScalar<XmlDocument>("'<xml/>'").InnerXml, Is.EqualTo("<xml />"));

				var xdoc = XDocument.Parse("<xml/>");
				var xml = Convert<string, XmlDocument>.Lambda("<xml/>");

				var d = conn.ExecuteScalarParameter<XDocument>(DataParameter.Xml("p", xdoc), "nvarchar(8000)");

				Assert.That(
					conn.ExecuteScalarParameter<string>("p", "nvarchar(8000)", "<xml/>"), Is.EqualTo("<xml/>"));
				Assert.That(
					conn.ExecuteScalarParameter<XDocument>(DataParameter.Xml("p", xdoc), "nvarchar(8000)").ToString(), Is.EqualTo("<xml />"));
				Assert.That(
					conn.ExecuteScalarParameter<XmlDocument>(DataParameter.Xml("p", xml), "nvarchar(8000)").InnerXml, Is.EqualTo("<xml />"));
				Assert.That(
					conn.ExecuteScalarParameter<XDocument>("p", "nvarchar(8000)", xdoc).ToString(), Is.EqualTo("<xml />"));
				Assert.That(
					conn.ExecuteScalarParameter<XDocument>("p", "nvarchar(8000)", xml).ToString(), Is.EqualTo("<xml />"));
			}
		}

		[Test]
		public void TestEnum1([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.ExecuteScalar<TestEnum>("'A'"), Is.EqualTo(TestEnum.AA));
				Assert.That(conn.ExecuteScalar<TestEnum?>("'A'"), Is.EqualTo(TestEnum.AA));
				Assert.That(conn.ExecuteScalar<TestEnum>("'B'"), Is.EqualTo(TestEnum.BB));
				Assert.That(conn.ExecuteScalar<TestEnum?>("'B'"), Is.EqualTo(TestEnum.BB));
			}
		}

		[Test]
		public void TestEnum2([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(
					conn.ExecuteScalarParameterObject<string>("p", "nvarchar(10)", new { p = TestEnum.AA }),
					Is.EqualTo("A"));
				Assert.That(
					conn.ExecuteScalarParameterObject<string>("p", "nvarchar(10)", new { p = (TestEnum?)TestEnum.BB }),
					Is.EqualTo("B"));
				Assert.That(
					conn.ExecuteScalarParameterObject<string>("p", "nvarchar(10)",
						new { p = ConvertTo<string>.From((TestEnum?)TestEnum.AA) }),
					Is.EqualTo("A"));
				Assert.That(
					conn.ExecuteScalarParameterObject<string>("p", "nvarchar(10)",
						new { p = ConvertTo<string>.From(TestEnum.AA) }),
					Is.EqualTo("A"));
				Assert.That(
					conn.ExecuteScalarParameterObject<string>("p", "nvarchar(10)",
						new { p = conn.MappingSchema.GetConverter<TestEnum?, string>()(TestEnum.AA) }),
					Is.EqualTo("A"));
			}
		}

		[Test]
		public void TestBinarySize([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					var data = new byte[500000];

					for (var i = 0; i < data.Length; i++)
						data[i] = (byte)(i % byte.MaxValue);

					conn.GetTable<ALLTYPE>().Insert(() => new ALLTYPE
					{
						INTDATATYPE = 2000,
						BLOBDATATYPE = data,
					});

					var blob = conn.GetTable<ALLTYPE>()
						.Where(t => t.INTDATATYPE == 2000)
						.Select(t => t.BLOBDATATYPE)
						.First();

					Assert.AreEqual(data, blob);
				}
				finally
				{
					conn.GetTable<ALLTYPE>().Delete(p => p.INTDATATYPE == 2000);
				}
			}
		}

		[Test]
		public void TestClobSize([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					var sb = new StringBuilder();

					for (var i = 0; i < 100000; i++)
						sb.Append(((char)((i % byte.MaxValue) + 32)).ToString());

					var data = sb.ToString();

					conn.GetTable<ALLTYPE>().Insert(() => new ALLTYPE
					{
						INTDATATYPE = 2000,
						CLOBDATATYPE = data,
					});

					var blob = conn.GetTable<ALLTYPE>()
						.Where(t => t.INTDATATYPE == 2000)
						.Select(t => t.CLOBDATATYPE)
						.First();

					Assert.AreEqual(data, blob);
				}
				finally
				{
					conn.GetTable<ALLTYPE>().Delete(p => p.INTDATATYPE == 2000);
				}
			}
		}

		[Table("AllTypes")]
		private class AllTypesNullable_Issue1287
		{
			[Column]
			public char? charDataType { get; set; }
		}

		[Test]
		public void TestNullableChar([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = GetDataContext(context))
			{
				var list = db.GetTable<AllTypesNullable_Issue1287>().Where(_ => _.charDataType == 'Y').ToList();

				Assert.AreEqual(1, list.Count);
				Assert.AreEqual('Y', list[0].charDataType);
			}
		}

		[Test]
		public void TestGuid([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(
					conn.Execute<Guid>("SELECT Cast('6F9619FF-8B86-D011-B42D-00C04FC964FF' as varchar(38))  FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Guid("6F9619FF-8B86-D011-B42D-00C04FC964FF")));

				Assert.That(
					conn.Execute<Guid?>("SELECT Cast('6F9619FF-8B86-D011-B42D-00C04FC964FF' as varchar(38)) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Guid("6F9619FF-8B86-D011-B42D-00C04FC964FF")));

				var guid = TestData.Guid1;

				if(TestProvNameDb2i.IsGAS(context))
				{
					Assert.That(conn.Execute<Guid>($"SELECT Cast({conn.GetParameterMarker("p")} as varchar(38)) FROM SYSIBM.SYSDUMMY1", DataParameter.Create("p", guid)), Is.EqualTo(guid));
					Assert.That(conn.Execute<Guid>($"SELECT Cast({conn.GetParameterMarker("p")} as varchar(38)) FROM SYSIBM.SYSDUMMY1", new DataParameter { Name = "p", Value = guid }), Is.EqualTo(guid));
				}
				else
				{
					Assert.That(conn.Execute<Guid>($"SELECT Cast({conn.GetParameterMarker("p")} as char(16) for bit data) FROM SYSIBM.SYSDUMMY1", DataParameter.Create("p", guid)), Is.EqualTo(guid));
					Assert.That(conn.Execute<Guid>($"SELECT Cast({conn.GetParameterMarker("p")} as char(16) for bit data) FROM SYSIBM.SYSDUMMY1", new DataParameter { Name = "p", Value = guid }), Is.EqualTo(guid));
				}
			}
		}

		[Table]
		class TestTimeTypes
		{
			[Column]
			public int Id { get; set; }

			[Column(DataType = DataType.Date)]
			public DateTime Date1 { get; set; }

			[Column(DbType = "Date")]
			public DateTime Date2 { get; set; }

			[Column]
			public TimeSpan Time { get; set; }

			// Precision must be explicitly stated from anything other than the default of 6.
			[Column(Precision = 0)]
			public DateTime TimeStamp0 { get; set; }

			[Column(Precision = 1)]
			public DateTime TimeStamp1 { get; set; }

			[Column(Precision = 2)]
			public DateTime TimeStamp2 { get; set; }

			[Column(Precision = 3)]
			public DateTime TimeStamp3 { get; set; }

			[Column(Precision = 4)]
			public DateTime TimeStamp4 { get; set; }

			[Column(Precision = 5)]
			public DateTime TimeStamp5 { get; set; }

			[Column(Precision = 6)]
			public DateTime TimeStamp6 { get; set; }

			[Column(Precision = 7)]
			public DateTime TimeStamp7 { get; set; }

			static TestTimeTypes()
			{
				Data = new[]
				{
					new TestTimeTypes() { Id = 1, Date1 = new DateTime(1234, 5, 6), Date2 = new DateTime(1234, 5, 7), Time = new TimeSpan(21, 2, 3) },
					new TestTimeTypes() { Id = 2, Date1 = new DateTime(6543, 2, 1), Date2 = new DateTime(1234, 5, 8), Time = new TimeSpan(23, 2, 1) }
				};

				for (var i = 1; i <= Data.Length; i++)
				{
					var idx = i - 1;
					Data[idx].TimeStamp0 = new DateTime(1000, 1, 10, 2, 20, 30 + i, 0);
					Data[idx].TimeStamp1 = new DateTime(1000, 1, 10, 2, 20, 30, i * 100);
					Data[idx].TimeStamp2 = new DateTime(1000, 1, 10, 2, 20, 30, i * 10);
					Data[idx].TimeStamp3 = new DateTime(1000, 1, 10, 2, 20, 30, i);
					Data[idx].TimeStamp4 = new DateTime(1000, 1, 10, 2, 20, 30, 1).AddTicks(1000 * i);
					Data[idx].TimeStamp5 = new DateTime(1000, 1, 10, 2, 20, 30, 1).AddTicks(100 * i);
					Data[idx].TimeStamp6 = new DateTime(1000, 1, 10, 2, 20, 30, 1).AddTicks(10 * i);
					Data[idx].TimeStamp7 = new DateTime(1000, 1, 10, 2, 20, 30, 1).AddTicks(1 * i);
				}
			}

			public static TestTimeTypes[] Data;

			public static Func<TestTimeTypes, TestTimeTypes, bool> Comparer = ComparerBuilder.GetEqualsFunc<TestTimeTypes>();
		}

		[Test]
		public void TestTimespanAndTimeValues([IncludeDataSources(false, TestProvNameDb2i.All)] string context, [Values] bool useParameters)
		{
			using (var db = GetDataContext(context))
			using (var table = db.CreateLocalTable(TestTimeTypes.Data))
			{
				db.InlineParameters = !useParameters;

				var record = table.Where(_ => _.Id == 1).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.Date1 == TestTimeTypes.Data[0].Date1).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.Date2 == TestTimeTypes.Data[0].Date2).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.Time == TestTimeTypes.Data[0].Time).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.TimeStamp0 == TestTimeTypes.Data[0].TimeStamp0).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.TimeStamp1 == TestTimeTypes.Data[0].TimeStamp1).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.TimeStamp2 == TestTimeTypes.Data[0].TimeStamp2).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.TimeStamp3 == TestTimeTypes.Data[0].TimeStamp3).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.TimeStamp4 == TestTimeTypes.Data[0].TimeStamp4).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.TimeStamp5 == TestTimeTypes.Data[0].TimeStamp5).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				record = table.Where(_ => _.TimeStamp6 == TestTimeTypes.Data[0].TimeStamp6).Single();
				Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));

				// older versions of the iSeries OS do not support timespans with precision > 6.
				if (TestProvNameDb2i.GetVersion(context) > LinqToDB.DataProvider.DB2iSeries.DB2iSeriesVersion.V7_2)
				{
					record = table.Where(_ => _.TimeStamp7 == TestTimeTypes.Data[0].TimeStamp7).Single();
					Assert.True(TestTimeTypes.Comparer(record, TestTimeTypes.Data[0]));
				}
			}
		}

		[Table]
		class TestParametersTable
		{
			[Column] public int Id { get; set; }
			[Column] public string Text { get; set; }
		}

		// https://github.com/linq2db/linq2db/issues/2091
		[Test]
		public void TestParametersUsed([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = new DataConnection(context))
			using (var table = db.CreateLocalTable<TestParametersTable>())
			{
				var newText = new TestParametersTable() { Id = 12, Text = "Hallo Welt!" };
				db.Insert(newText);
				
				var text = "bla";
				var query = from f in table where f.Text == text select f;
				var result = query.ToArray();

				Assert.True(db.LastQuery!.Contains(db.GetParameterMarker("")));
			}
		}

		[Test]
		public void TestGuidToString([DataSources] string context)
		{
			// GUIDs are serialized in lower case.
			var guid = !IsCaseSensitiveDB(context) ? "FEBE3ECA-CB5F-40B2-AD39-2979D312AFCA" : "febe3eca-cb5f-40b2-ad39-2979d312afca";

			using (var db = GetDataContext(context))
			{
				var expected = from t in Types where Sql.ConvertTo<string>.From(t.GuidValue) == "febe3eca-cb5f-40b2-ad39-2979d312afca" select t.GuidValue;
				var actual = from t in db.Types where Sql.ConvertTo<string>.From(t.GuidValue) == guid select t.GuidValue;

				AreEqual(expected, actual);
			}
		}
	}
}
