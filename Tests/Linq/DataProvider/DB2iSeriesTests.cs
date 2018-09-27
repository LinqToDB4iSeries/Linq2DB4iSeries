using System;
using System.Data.Linq;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.DataProvider;
using LinqToDB.DataProvider.DB2iSeries;
using LinqToDB.Mapping;

using IBM.Data.DB2.iSeries;

using NUnit.Framework;

namespace Tests.DataProvider
{
    using IBM.Data.DB2Types;
    using Model;

	[TestFixture]
	public class DB2iSeriesTests : TestBase
	{
		[Test, DataContextSource(false)]
		public void TestParameters(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.Execute<string>("SELECT cast(@p as varchar(10)) FROM SYSIBM.SYSDUMMY1", new {p = 1}),
					Is.EqualTo("1"));
				Assert.That(conn.Execute<string>("SELECT cast(@p as varchar(10)) FROM SYSIBM.SYSDUMMY1", new {p = "1"}),
					Is.EqualTo("1"));
				Assert.That(
					conn.Execute<int>("SELECT cast(@p as varchar(10)) FROM SYSIBM.SYSDUMMY1", new {p = new DataParameter {Value = 1}}),
					Is.EqualTo(1));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p1 as varchar(10)) FROM SYSIBM.SYSDUMMY1",
						new {p1 = new DataParameter {Value = "1"}}), Is.EqualTo("1"));
				Assert.That(
					conn.Execute<int>("SELECT cast(@p1 as int) + cast(@p2 as int) FROM SYSIBM.SYSDUMMY1", new {p1 = 2, p2 = 3}),
					Is.EqualTo(5));
				Assert.That(
					conn.Execute<int>("SELECT cast(@p2 as int) + cast(@p1 as int) FROM SYSIBM.SYSDUMMY1", new {p2 = 2, p1 = 3}),
					Is.EqualTo(5));
			}
		}

		protected string GetNullSql = "SELECT {0} FROM {1} WHERE ID = 1";
		protected string GetValueSql = "SELECT {0} FROM {1} WHERE ID = 2";
		protected string PassNullSql = "SELECT ID FROM {1} WHERE @p IS NULL AND {0} IS NULL OR @p1 IS NOT NULL AND {0} = @p2";
		protected string PassValueSql = "SELECT ID FROM {1} WHERE {0} = @p";

		protected T TestType<T>(DataConnection conn, string fieldName,
			DataType dataType = DataType.Undefined,
			string tableName = "AllTypes",
			bool skipPass = false,
			bool skipNull = false,
			bool skipDefinedNull = false,
			bool skipDefaultNull = false,
			bool skipUndefinedNull = false,
			bool skipNotNull = false,
			bool skipDefined = false,
			bool skipDefault = false,
			bool skipUndefined = false)
		{
			var type = typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>)
				? typeof(T).GetGenericArguments()[0]
				: typeof(T);

			// Get NULL value.
			//
			Debug.WriteLine("{0} {1}:{2} -> NULL", fieldName, (object) type.Name, dataType);

			var sql = string.Format(GetNullSql, fieldName, tableName);
			var value = conn.Execute<T>(sql);
			var def = conn.MappingSchema.GetDefaultValue(typeof(T));
			Assert.That(value, Is.EqualTo(def));

			int? id;

			if (!skipNull && !skipPass && PassNullSql != null)
			{
				sql = string.Format(PassNullSql, fieldName, tableName);

				if (!skipDefinedNull && dataType != DataType.Undefined)
				{
					// Get NULL ID with dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> NULL ID with dataType", fieldName, (object) type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value, dataType), new DataParameter("p1", value, dataType),
						new DataParameter("p2", value, dataType));
					Assert.That(id, Is.EqualTo(1));
				}

				if (!skipDefaultNull)
				{
					// Get NULL ID with default dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> NULL ID with default dataType", fieldName, (object) type.Name, dataType);
					id = conn.Execute<int?>(sql, new {p = value, p1 = value, p2 = value});
					Assert.That(id, Is.EqualTo(1));
				}

				if (!skipUndefinedNull)
				{
					// Get NULL ID without dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> NULL ID without dataType", fieldName, (object) type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value, dataType), new DataParameter("p1", value, dataType),
						new DataParameter("p2", value, dataType));
					Assert.That(id, Is.EqualTo(1));
				}
			}

			// Get value.
			//
			Debug.WriteLine("{0} {1}:{2} -> value", fieldName, (object) type.Name, dataType);
			sql = string.Format(GetValueSql, fieldName, tableName);
			value = conn.Execute<T>(sql);

			if (!skipNotNull && !skipPass)
			{
				sql = string.Format(PassValueSql, fieldName, tableName);

				if (!skipDefined && dataType != DataType.Undefined)
				{
					// Get value ID with dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> value ID with dataType", fieldName, (object) type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value, dataType));
					Assert.That(id, Is.EqualTo(2));
				}

				if (!skipDefault)
				{
					// Get value ID with default dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> value ID with default dataType", fieldName, (object) type.Name, dataType);
					id = conn.Execute<int?>(sql, new {p = value});
					Assert.That(id, Is.EqualTo(2));
				}

				if (!skipUndefined)
				{
					// Get value ID without dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> value ID without dataType", fieldName, (object) type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value));
					Assert.That(id, Is.EqualTo(2));
				}
			}

			return value;
		}


		[Test, DataContextSource(false)]
		public void TestDataTypes(string context)
		{
			using (var conn = new DataConnection(context))
			{
                if (context.Contains("DB2Connect"))
                {
                    Assert.That(TestType<long?>(conn, "bigintDataType", DataType.Int64), Is.EqualTo(1000000L));
                    Assert.That(TestType<DB2Int64?>(conn, "bigintDataType", DataType.Int64), Is.EqualTo(new DB2Int64(1000000L)));
                    Assert.That(TestType<int?>(conn, "intDataType", DataType.Int32), Is.EqualTo(444444));
                    Assert.That(TestType<DB2Int32?>(conn, "intDataType", DataType.Int32), Is.EqualTo(new DB2Int32(444444)));
                    Assert.That(TestType<short?>(conn, "smallintDataType", DataType.Int16), Is.EqualTo(100));
                    Assert.That(TestType<DB2Int16?>(conn, "smallintDataType", DataType.Int16), Is.EqualTo(new DB2Int16(100)));
                    Assert.That(TestType<decimal?>(conn, "decimalDataType", DataType.Decimal), Is.EqualTo(666m));
                    Assert.That(TestType<decimal?>(conn, "decfloat16DataType", DataType.Decimal), Is.EqualTo(888.456m));
                    Assert.That(TestType<decimal?>(conn, "decfloat34DataType", DataType.Decimal), Is.EqualTo(777.987m));
                    Assert.That(TestType<float?>(conn, "realDataType", DataType.Single), Is.EqualTo(222.987f));
                    Assert.That(TestType<DB2Real?>(conn, "realDataType", DataType.Single), Is.EqualTo(new DB2Real(222.987f)));
                    Assert.That(TestType<double?>(conn, "doubleDataType", DataType.Double), Is.EqualTo(555.987d));
                    Assert.That(TestType<DB2Double?>(conn, "doubleDataType", DataType.Double), Is.EqualTo(new DB2Double(555.987d)));

                    Assert.That(TestType<string>(conn, "charDataType", DataType.Char), Is.EqualTo("Y"));
                    Assert.That(TestType<string>(conn, "charDataType", DataType.NChar), Is.EqualTo("Y"));
                    Assert.That(TestType<DB2String?>(conn, "charDataType", DataType.Char), Is.EqualTo(new DB2String("Y")));
                    Assert.That(TestType<string>(conn, "varcharDataType", DataType.VarChar), Is.EqualTo("var-char"));
                    Assert.That(TestType<string>(conn, "varcharDataType", DataType.NVarChar), Is.EqualTo("var-char"));
                    Assert.That(TestType<string>(conn, "clobDataType", DataType.Text), Is.EqualTo("567"));
                    Assert.That(TestType<string>(conn, "dbclobDataType", DataType.NText), Is.EqualTo("890"));

                    Assert.That(TestType<byte[]>(conn, "binaryDataType", DataType.Binary), Is.EqualTo(
                        new byte[] { 0xF1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));
                    Assert.That(TestType<byte[]>(conn, "varbinaryDataType", DataType.VarBinary), Is.EqualTo(new byte[] { 0xF4 }));
                    Assert.That(TestType<byte[]>(conn, "blobDataType", DataType.Blob, skipDefaultNull: true, skipUndefinedNull: true, skipDefault: true, skipUndefined: true), Is.EqualTo(new byte[] { 0xF2, 0xF3, 0xF4 }));
                    Assert.That(TestType<byte[]>(conn, "blobDataType", DataType.VarBinary, skipDefaultNull: true, skipUndefinedNull: true, skipDefault: true, skipUndefined: true), Is.EqualTo(new byte[] { 0xF2, 0xF3, 0xF4 }));
                    Assert.That(TestType<string>(conn, "graphicDataType", DataType.VarChar), Is.EqualTo("graphic   "));
                    Assert.That(TestType<string>(conn, "vargraphicDataType", DataType.VarChar), Is.EqualTo("vargraphic"));

                    Assert.That(TestType<DateTime?>(conn, "dateDataType", DataType.Date), Is.EqualTo(new DateTime(2012, 12, 12)));
                    Assert.That(TestType<DB2Date?>(conn, "dateDataType", DataType.Date), Is.EqualTo(new DB2Date(new DateTime(2012, 12, 12))));
                    Assert.That(TestType<TimeSpan?>(conn, "timeDataType", DataType.Time), Is.EqualTo(new TimeSpan(12, 12, 12)));
                    Assert.That(TestType<DB2Time?>(conn, "timeDataType", DataType.Time), Is.EqualTo(new DB2Time(new TimeSpan(12, 12, 12))));
                    Assert.That(TestType<DateTime?>(conn, "timestampDataType", DataType.DateTime2), Is.EqualTo(new DateTime(2012, 12, 12, 12, 12, 12, 0)));
                    Assert.That(TestType<DB2TimeStamp?>(conn, "timestampDataType", DataType.DateTime2), Is.EqualTo(new DB2TimeStamp(new DateTime(2012, 12, 12, 12, 12, 12, 0))));

                    Assert.That(TestType<string>(conn, "xmlDataType", DataType.Xml, skipPass: true), Is.EqualTo("<root><element strattr=\"strvalue\" intattr=\"12345\"/></root>"));

                    Assert.That(conn.Execute<byte[]>("SELECT rowidDataType FROM AllTypes WHERE ID = 2").Length, Is.Not.EqualTo(0));
                    //Assert.That(conn.Execute<DB2RowId>("SELECT rowid FROM AllTypes WHERE ID = 2").Value.Length, Is.Not.EqualTo(0));

                    //TestType<DB2Clob>(conn, "clobDataType", DataType.Text, skipNotNull: true);
                    //TestType<DB2Blob>(conn, "blobDataType", DataType.VarBinary, skipNotNull: true);
                    //TestType<DB2Xml>(conn, "xmlDataType", DataType.Xml, skipPass: true);

                    Assert.That(TestType<DB2Decimal?>(conn, "decimalDataType", DataType.Decimal).ToString(),
                        Is.EqualTo(new DB2Decimal(666m).ToString()));
                    Assert.That(TestType<DB2Binary>(conn, "varbinaryDataType", DataType.VarBinary).ToString(),
                        Is.EqualTo(new DB2Binary(new byte[] { 0xF4 }).ToString()));
                    Assert.That(TestType<DB2DecimalFloat?>(conn, "decfloat16DataType", DataType.Decimal),
                        Is.EqualTo(new DB2DecimalFloat(888.456m)));
                    Assert.That(TestType<DB2DecimalFloat?>(conn, "decfloat34DataType", DataType.Decimal).ToString(),
                        Is.EqualTo(new DB2DecimalFloat(777.987m).ToString()));
                }
                else
                {
                    Assert.That(TestType<long?>(conn, "bigintDataType", DataType.Int64), Is.EqualTo(1000000L));
                    Assert.That(TestType<iDB2BigInt?>(conn, "bigintDataType", DataType.Int64), Is.EqualTo(new iDB2BigInt(1000000L)));
                    Assert.That(TestType<int?>(conn, "intDataType", DataType.Int32), Is.EqualTo(444444));
                    Assert.That(TestType<iDB2Integer?>(conn, "intDataType", DataType.Int32), Is.EqualTo(new iDB2Integer(444444)));
                    Assert.That(TestType<short?>(conn, "smallintDataType", DataType.Int16), Is.EqualTo(100));
                    Assert.That(TestType<iDB2SmallInt?>(conn, "smallintDataType", DataType.Int16), Is.EqualTo(new iDB2SmallInt(100)));
                    Assert.That(TestType<decimal?>(conn, "decimalDataType", DataType.Decimal), Is.EqualTo(666m));
                    Assert.That(TestType<decimal?>(conn, "decfloat16DataType", DataType.Decimal), Is.EqualTo(888.456m));
                    Assert.That(TestType<decimal?>(conn, "decfloat34DataType", DataType.Decimal), Is.EqualTo(777.987m));
                    Assert.That(TestType<float?>(conn, "realDataType", DataType.Single), Is.EqualTo(222.987f));
                    Assert.That(TestType<iDB2Real?>(conn, "realDataType", DataType.Single), Is.EqualTo(new iDB2Real(222.987f)));
                    Assert.That(TestType<double?>(conn, "doubleDataType", DataType.Double), Is.EqualTo(555.987d));
                    Assert.That(TestType<iDB2Double?>(conn, "doubleDataType", DataType.Double), Is.EqualTo(new iDB2Double(555.987d)));

                    Assert.That(TestType<string>(conn, "charDataType", DataType.Char), Is.EqualTo("Y"));
                    Assert.That(TestType<string>(conn, "charDataType", DataType.NChar), Is.EqualTo("Y"));
                    Assert.That(TestType<iDB2VarChar?>(conn, "charDataType", DataType.Char), Is.EqualTo(new iDB2VarChar("Y")));
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

                    Assert.That(TestType<string>(conn, "graphicDataType", DataType.VarChar), Is.EqualTo("graphic   "));
                    Assert.That(TestType<string>(conn, "vargraphicDataType", DataType.VarChar), Is.EqualTo("vargraphic"));

                    Assert.That(TestType<DateTime?>(conn, "dateDataType", DataType.Date, skipDefault: true, skipUndefined: true),
                        Is.EqualTo(new DateTime(2012, 12, 12)));
                    Assert.That(TestType<iDB2Date?>(conn, "dateDataType", DataType.Date),
                        Is.EqualTo(new iDB2Date(new DateTime(2012, 12, 12))));
                    Assert.That(TestType<TimeSpan?>(conn, "timeDataType", DataType.Time), Is.EqualTo(new TimeSpan(12, 12, 12)));
                    Assert.That(TestType<iDB2Time?>(conn, "timeDataType", DataType.Time),
                        Is.EqualTo(new iDB2Time(new DateTime(1, 1, 1, 12, 12, 12))));
                    Assert.That(
                        TestType<DateTime?>(conn, "timestampDataType", DataType.DateTime2, skipDefault: true, skipUndefined: true),
                        Is.EqualTo(new DateTime(2012, 12, 12, 12, 12, 12, 0)));
                    Assert.That(TestType<iDB2TimeStamp?>(conn, "timestampDataType", DataType.DateTime2),
                        Is.EqualTo(new iDB2TimeStamp(new DateTime(2012, 12, 12, 12, 12, 12, 0))));

                    Assert.That(TestType<string>(conn, "xmlDataType", DataType.Xml, skipPass: true),
                        Is.EqualTo("<root><element strattr=\"strvalue\" intattr=\"12345\"/></root>"));

                    Assert.That(conn.Execute<byte[]>("SELECT rowidDataType FROM AllTypes WHERE ID = 2").Length, Is.Not.EqualTo(0));
                    Assert.That(conn.Execute<iDB2Rowid>("SELECT rowidDataType FROM AllTypes WHERE ID = 2").Value.Length,
                        Is.Not.EqualTo(0));

                    TestType<iDB2Clob>(conn, "clobDataType", DataType.Text, skipNotNull: true);
                    TestType<iDB2Blob>(conn, "blobDataType", DataType.VarBinary, skipNotNull: true);

                    TestType<iDB2Xml>(conn, "xmlDataType", DataType.Xml, skipPass: true);

                    Assert.That(TestType<iDB2Decimal?>(conn, "decimalDataType", DataType.Decimal).ToString(),
                        Is.EqualTo(new iDB2Decimal(666m).ToString()));
                    Assert.That(TestType<iDB2Binary>(conn, "varbinaryDataType", DataType.VarBinary).ToString(),
                        Is.EqualTo(new iDB2Binary(new byte[] { 0xF4 }).ToString()));
                    Assert.That(TestType<iDB2DecFloat16?>(conn, "decfloat16DataType", DataType.Decimal),
                        Is.EqualTo(new iDB2DecFloat16(888.456m)));
                    Assert.That(TestType<iDB2DecFloat34?>(conn, "decfloat34DataType", DataType.Decimal).ToString(),
                        Is.EqualTo(new iDB2DecFloat34(777.987m).ToString()));
                }
			}
		}

		static void TestNumeric<T>(DataConnection conn, T expectedValue, DataType dataType, string skip = "")
		{
			var skipTypes = skip.Split(' ');

			foreach (var sqlType in new[]
			{
				"bigint",
				"int",
				"smallint",
				"decimal(31)",
				"decfloat",
				"double",
				"real"
			}.Except(skipTypes))
			{
				var sqlValue = expectedValue is bool ? (bool) (object) expectedValue ? 1 : 0 : (object) expectedValue;
                var sql = string.Format(System.Globalization.CultureInfo.InvariantCulture, "SELECT Cast({0} as {1}) FROM SYSIBM.SYSDUMMY1", sqlValue ?? "NULL", sqlType);
				Debug.WriteLine(sql + " -> " + typeof(T));
				Assert.That(conn.Execute<T>(sql), Is.EqualTo(expectedValue));
			}

			string castType = "real";

			switch (dataType)
			{
				case DataType.Boolean:
				case DataType.Int16:
				case DataType.Int32:
				case DataType.Int64:
				case DataType.UInt32:
					castType = "bigint";
					break;
				case DataType.UInt16:
					castType = "int";
					break;
				case DataType.UInt64:
					castType = "decimal(20,0)";
					break;
				case DataType.Single:
					castType = "real";
					break;
				case DataType.Double:
					castType = "float(34)";
					break;
				case DataType.VarNumeric:
				case DataType.Decimal:
					if (expectedValue != null)
					{
						var val = expectedValue.ToString();
						int precision = val.Replace("-", "").Replace(".", "").Length;
						int point = val.IndexOf(".");
						int scale = point < 0 ? 0 : val.Length - point;
						castType = string.Format("decimal({0},{1})", precision, scale);
					}
					else
						castType = "decimal";

					break;
				case DataType.Money:
					castType = "decfloat";
					break;
			}


			Debug.WriteLine("{0} -> DataType.{1}", typeof(T), dataType);
			string sql1 = $"SELECT cast(@p as {castType}) FROM SYSIBM.SYSDUMMY1";

			Assert.That(conn.Execute<T>(sql1, new DataParameter {Name = "p", DataType = dataType, Value = expectedValue}),
				Is.EqualTo(expectedValue));
			Debug.WriteLine("{0} -> auto", typeof(T));
			Assert.That(
				conn.Execute<T>($"SELECT cast(@p as {castType}) FROM SYSIBM.SYSDUMMY1",
					new DataParameter {Name = "p", Value = expectedValue}), Is.EqualTo(expectedValue));
			Debug.WriteLine("{0} -> new", typeof(T));
			Assert.That(conn.Execute<T>($"SELECT cast(@p as {castType}) FROM SYSIBM.SYSDUMMY1", new {p = expectedValue}),
				Is.EqualTo(expectedValue));
		}

		static void TestSimple<T>(DataConnection conn, T expectedValue, DataType dataType)
			where T : struct
		{
			TestNumeric<T>(conn, expectedValue, dataType);
			TestNumeric<T?>(conn, expectedValue, dataType);
			TestNumeric<T?>(conn, (T?) null, dataType);
		}

		[Test, DataContextSource(false)]
		public void TestNumerics(string context)
		{
			using (var conn = new DataConnection(context))
			{
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
				TestSimple<decimal>(conn, 1m, DataType.VarNumeric);
				TestSimple<decimal>(conn, 1m, DataType.Money);
				TestSimple<decimal>(conn, 1m, DataType.SmallMoney);

				TestNumeric(conn, sbyte.MinValue, DataType.SByte);
				TestNumeric(conn, sbyte.MaxValue, DataType.SByte);
				TestNumeric(conn, short.MinValue, DataType.Int16);
				TestNumeric(conn, short.MaxValue, DataType.Int16);
				TestNumeric(conn, int.MinValue, DataType.Int32, "smallint");
				TestNumeric(conn, int.MaxValue, DataType.Int32, "smallint real");
				TestNumeric(conn, long.MinValue, DataType.Int64, "smallint int double");
				TestNumeric(conn, long.MaxValue, DataType.Int64, "smallint int double real");
				TestNumeric(conn, byte.MaxValue, DataType.Byte);
				TestNumeric(conn, ushort.MaxValue, DataType.UInt16, "smallint");
				TestNumeric(conn, uint.MaxValue, DataType.UInt32, "smallint int real");
				TestNumeric(conn, ulong.MaxValue, DataType.UInt64, "smallint int real bigint double");
				TestNumeric(conn, -3.40282306E+38f, DataType.Single, "bigint int smallint decimal(31) decfloat");
				TestNumeric(conn, 3.40282306E+38f, DataType.Single, "bigint int smallint decimal(31) decfloat");
				TestNumeric(conn, -1.79E+308d, DataType.Double, "bigint int smallint decimal(31) decfloat real");
				TestNumeric(conn, 1.79E+308d, DataType.Double, "bigint int smallint decimal(31) decfloat real");
				TestNumeric(conn, decimal.MinValue, DataType.Decimal, "bigint int smallint double real");
				TestNumeric(conn, decimal.MaxValue, DataType.Decimal, "bigint int smallint double real");
				TestNumeric(conn, decimal.MinValue, DataType.VarNumeric, "bigint int smallint double real");
				TestNumeric(conn, decimal.MaxValue, DataType.VarNumeric, "bigint int smallint double real");
				TestNumeric(conn, -922337203685477m, DataType.Money, "int smallint real");
				TestNumeric(conn, +922337203685477m, DataType.Money, "int smallint real");
				TestNumeric(conn, -214748m, DataType.SmallMoney, "smallint");
				TestNumeric(conn, +214748m, DataType.SmallMoney, "smallint");
			}
		}

		[Test, DataContextSource(false)]
		public void TestDate(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var dateTime = new DateTime(2012, 12, 12);

				Assert.That(conn.Execute<DateTime>("SELECT Cast('2012-12-12' as date) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(dateTime));
				Assert.That(conn.Execute<DateTime?>("SELECT Cast('2012-12-12' as date) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(dateTime));
				Assert.That(
					conn.Execute<DateTime>("SELECT cast(@p as date) FROM SYSIBM.SYSDUMMY1", DataParameter.Date("p", dateTime)),
					Is.EqualTo(dateTime));
				Assert.That(
					conn.Execute<DateTime?>("SELECT cast(@p as date) FROM SYSIBM.SYSDUMMY1",
						new DataParameter("p", dateTime, DataType.Date)), Is.EqualTo(dateTime));
			}
		}

		[Test, DataContextSource(false)]
		public void TestDateTime(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var dateTime = new DateTime(2012, 12, 12, 12, 12, 12);

				Assert.That(conn.Execute<DateTime>("SELECT Cast('2012-12-12 12:12:12' as timestamp) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(dateTime));
				Assert.That(conn.Execute<DateTime?>("SELECT Cast('2012-12-12 12:12:12' as timestamp) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(dateTime));

				Assert.That(
					conn.Execute<DateTime>("SELECT cast(@p as timestamp) FROM SYSIBM.SYSDUMMY1",
						DataParameter.DateTime("p", dateTime)), Is.EqualTo(dateTime));
				Assert.That(
					conn.Execute<DateTime?>("SELECT cast(@p as timestamp) FROM SYSIBM.SYSDUMMY1", new DataParameter("p", dateTime)),
					Is.EqualTo(dateTime));
				Assert.That(
					conn.Execute<DateTime?>("SELECT cast(@p as timestamp) FROM SYSIBM.SYSDUMMY1",
						new DataParameter("p", dateTime, DataType.DateTime)), Is.EqualTo(dateTime));
			}
		}

		[Test, DataContextSource(false)]
		public void TestTimeSpan(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var time = new TimeSpan(12, 12, 12);

				Assert.That(conn.Execute<TimeSpan>("SELECT Cast('12:12:12' as time) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo(time));
				Assert.That(conn.Execute<TimeSpan?>("SELECT Cast('12:12:12' as time) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo(time));

				Assert.That(conn.Execute<TimeSpan>("SELECT cast(@p as time) FROM SYSIBM.SYSDUMMY1", DataParameter.Time("p", time)),
					Is.EqualTo(time));
				Assert.That(
					conn.Execute<TimeSpan>("SELECT cast(@p as time) FROM SYSIBM.SYSDUMMY1", DataParameter.Create("p", time)),
					Is.EqualTo(time));
				Assert.That(
					conn.Execute<TimeSpan?>("SELECT cast(@p as time) FROM SYSIBM.SYSDUMMY1",
						new DataParameter("p", time, DataType.Time)), Is.EqualTo(time));
				Assert.That(conn.Execute<TimeSpan?>("SELECT cast(@p as time) FROM SYSIBM.SYSDUMMY1", new DataParameter("p", time)),
					Is.EqualTo(time));
			}
		}

		[Test, DataContextSource(false)]
		public void TestChar(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.Execute<char>("SELECT Cast('1' as char) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));
				Assert.That(conn.Execute<char?>("SELECT Cast('1' as char) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));
				Assert.That(conn.Execute<char>("SELECT Cast('1' as char(1)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));
				Assert.That(conn.Execute<char?>("SELECT Cast('1' as char(1)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));

				Assert.That(conn.Execute<char>("SELECT Cast('1' as varchar(1)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));
				Assert.That(conn.Execute<char?>("SELECT Cast('1' as varchar(1)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));
				Assert.That(conn.Execute<char>("SELECT Cast('1' as varchar(20)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));
				Assert.That(conn.Execute<char?>("SELECT Cast('1' as varchar(20)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo('1'));

				Assert.That(conn.Execute<char>("SELECT Cast(@p as char) FROM SYSIBM.SYSDUMMY1", DataParameter.Char("p", '1')),
					Is.EqualTo('1'));
				Assert.That(conn.Execute<char?>("SELECT Cast(@p as char) FROM SYSIBM.SYSDUMMY1", DataParameter.Char("p", '1')),
					Is.EqualTo('1'));
				Assert.That(conn.Execute<char>("SELECT Cast(@p as char(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.Char("p", '1')),
					Is.EqualTo('1'));
				Assert.That(conn.Execute<char?>("SELECT Cast(@p as char(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.Char("p", '1')),
					Is.EqualTo('1'));

				Assert.That(
					conn.Execute<char>("SELECT CAST(@p as varchar(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.VarChar("p", '1')),
					Is.EqualTo('1'));
				Assert.That(
					conn.Execute<char?>("SELECT CAST(@p as varchar(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.VarChar("p", '1')),
					Is.EqualTo('1'));
				Assert.That(conn.Execute<char>("SELECT CAST(@p as nchar(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.NChar("p", '1')),
					Is.EqualTo('1'));
				Assert.That(conn.Execute<char?>("SELECT CAST(@p as nchar(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.NChar("p", '1')),
					Is.EqualTo('1'));
				Assert.That(
					conn.Execute<char>("SELECT CAST(@p as nvarchar(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.NVarChar("p", '1')),
					Is.EqualTo('1'));
				Assert.That(
					conn.Execute<char?>("SELECT CAST(@p as nvarchar(1)) FROM SYSIBM.SYSDUMMY1", DataParameter.NVarChar("p", '1')),
					Is.EqualTo('1'));
				Assert.That(
					conn.Execute<char>("SELECT CAST(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1", DataParameter.Create("p", '1')),
					Is.EqualTo('1'));
				Assert.That(
					conn.Execute<char?>("SELECT CAST(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1", DataParameter.Create("p", '1')),
					Is.EqualTo('1'));

				Assert.That(
					conn.Execute<char>("SELECT CAST(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1",
						new DataParameter {Name = "p", Value = '1'}), Is.EqualTo('1'));
				Assert.That(
					conn.Execute<char?>("SELECT CAST(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1",
						new DataParameter {Name = "p", Value = '1'}), Is.EqualTo('1'));
			}
		}

		[Test, DataContextSource(false)]
		public void TestString(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.Execute<string>("SELECT Cast('12345' as char(5)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo("12345"));
				Assert.That(conn.Execute<string>("SELECT Cast('12345' as char(20)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo("12345"));
				Assert.That(conn.Execute<string>("SELECT Cast(NULL    as char(20)) FROM SYSIBM.SYSDUMMY1"), Is.Null);

				Assert.That(conn.Execute<string>("SELECT Cast('12345' as varchar(5)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo("12345"));
				Assert.That(conn.Execute<string>("SELECT Cast('12345' as varchar(20)) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo("12345"));
				Assert.That(conn.Execute<string>("SELECT Cast(NULL    as varchar(20)) FROM SYSIBM.SYSDUMMY1"), Is.Null);

				Assert.That(conn.Execute<string>("SELECT Cast('12345' as clob) FROM SYSIBM.SYSDUMMY1"), Is.EqualTo("12345"));
				Assert.That(conn.Execute<string>("SELECT Cast(NULL    as clob) FROM SYSIBM.SYSDUMMY1"), Is.Null);

				Assert.That(
					conn.Execute<string>("SELECT cast(@p as char(5)) FROM SYSIBM.SYSDUMMY1", DataParameter.Char("p", "123")),
					Is.EqualTo("123"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as varchar(10)) FROM SYSIBM.SYSDUMMY1", DataParameter.VarChar("p", "123")),
					Is.EqualTo("123"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as varchar(10)) FROM SYSIBM.SYSDUMMY1", DataParameter.Text("p", "123")),
					Is.EqualTo("123"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nchar(5)) FROM SYSIBM.SYSDUMMY1", DataParameter.NChar("p", "123")),
					Is.EqualTo("123  "));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1", DataParameter.NVarChar("p", "123")),
					Is.EqualTo("123"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1", DataParameter.NText("p", "123")),
					Is.EqualTo("123"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1", DataParameter.Create("p", "123")),
					Is.EqualTo("123"));

				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1",
						DataParameter.Create("p", (string) null)), Is.EqualTo(null));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1",
						new DataParameter {Name = "p", Value = "1"}), Is.EqualTo("1"));
			}
		}

		[Test, DataContextSource(false)]
		public void TestBinary(string context)
		{
			// results are going to be bytes from EDCIDC character set
			var arr1 = new byte[] {241, 242};
			var arr2 = new byte[] {241, 242, 243, 244};

			using (var conn = new DataConnection(context))
			{
				var res1 = conn.Execute<byte[]>("SELECT Cast('12' as char(2) for bit data) FROM SYSIBM.SYSDUMMY1");
				Assert.That(res1, Is.EqualTo(arr1));
				Assert.That(conn.Execute<Binary>("SELECT Cast('1234' as char(4) for bit data) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Binary(arr2)));

				Assert.That(conn.Execute<byte[]>("SELECT Cast('12' as varchar(2) for bit data) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(arr1));
				Assert.That(conn.Execute<Binary>("SELECT Cast('1234' as varchar(4) for bit data) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Binary(arr2)));
			}
		}

		[Test, IncludeDataContextSource(DB2iSeriesProviderName.DB2_73, DB2iSeriesProviderName.DB2)]
		public void TestGuidBlob(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(
					conn.Execute<Guid>("SELECT Cast('6F9619FF-8B86-D011-B42D-00C04FC964FF' as varchar(38))  FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Guid("6F9619FF-8B86-D011-B42D-00C04FC964FF")));

				Assert.That(
					conn.Execute<Guid?>("SELECT Cast('6F9619FF-8B86-D011-B42D-00C04FC964FF' as varchar(38)) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Guid("6F9619FF-8B86-D011-B42D-00C04FC964FF")));

				var guid = Guid.NewGuid();

				Assert.That(
					conn.Execute<Guid>("SELECT Cast(@p as char(16) for bit data) FROM SYSIBM.SYSDUMMY1",
						DataParameter.Create("p", guid)), Is.EqualTo(guid));
				Assert.That(
					conn.Execute<Guid>("SELECT Cast(@p as char(16) for bit data) FROM SYSIBM.SYSDUMMY1",
						new DataParameter {Name = "p", Value = guid}), Is.EqualTo(guid));
			}
		}


		[Test, IncludeDataContextSource(DB2iSeriesProviderName.DB2_73_GAS, DB2iSeriesProviderName.DB2_GAS)]
		public void TestGuidAsString(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(
					conn.Execute<Guid>("SELECT Cast('6F9619FF-8B86-D011-B42D-00C04FC964FF' as varchar(38))  FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Guid("6F9619FF-8B86-D011-B42D-00C04FC964FF")));

				Assert.That(
					conn.Execute<Guid?>("SELECT Cast('6F9619FF-8B86-D011-B42D-00C04FC964FF' as varchar(38)) FROM SYSIBM.SYSDUMMY1"),
					Is.EqualTo(new Guid("6F9619FF-8B86-D011-B42D-00C04FC964FF")));
			}
		}


		[Test, DataContextSource(false)]
		public void TestXml(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.Execute<string>("SELECT '<xml/>' FROM SYSIBM.SYSDUMMY1"), Is.EqualTo("<xml/>"));
				Assert.That(conn.Execute<XDocument>("SELECT '<xml/>' FROM SYSIBM.SYSDUMMY1").ToString(), Is.EqualTo("<xml />"));
				Assert.That(conn.Execute<XmlDocument>("SELECT '<xml/>' FROM SYSIBM.SYSDUMMY1").InnerXml, Is.EqualTo("<xml />"));

				var xdoc = XDocument.Parse("<xml/>");
				var xml = Convert<string, XmlDocument>.Lambda("<xml/>");

				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(8000)) FROM SYSIBM.SYSDUMMY1", DataParameter.Xml("p", "<xml/>")),
					Is.EqualTo("<xml/>"));
				Assert.That(
					conn.Execute<XDocument>("SELECT cast(@p as nvarchar(8000)) FROM SYSIBM.SYSDUMMY1", DataParameter.Xml("p", xdoc))
						.ToString(), Is.EqualTo("<xml />"));
				Assert.That(
					conn.Execute<XmlDocument>("SELECT cast(@p as nvarchar(8000)) FROM SYSIBM.SYSDUMMY1", DataParameter.Xml("p", xml))
						.InnerXml, Is.EqualTo("<xml />"));
				Assert.That(
					conn.Execute<XDocument>("SELECT cast(@p as nvarchar(8000)) FROM SYSIBM.SYSDUMMY1", new DataParameter("p", xdoc))
						.ToString(), Is.EqualTo("<xml />"));
				Assert.That(
					conn.Execute<XDocument>("SELECT cast(@p as nvarchar(8000)) FROM SYSIBM.SYSDUMMY1", new DataParameter("p", xml))
						.ToString(), Is.EqualTo("<xml />"));
			}
		}

		enum TestEnum
		{
			[MapValue("A")] AA,
			[MapValue("B")] BB,
		}

		[Test, DataContextSource(false)]
		public void TestEnum1(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.Execute<TestEnum>("SELECT 'A' FROM SYSIBM.SYSDUMMY1"), Is.EqualTo(TestEnum.AA));
				Assert.That(conn.Execute<TestEnum?>("SELECT 'A' FROM SYSIBM.SYSDUMMY1"), Is.EqualTo(TestEnum.AA));
				Assert.That(conn.Execute<TestEnum>("SELECT 'B' FROM SYSIBM.SYSDUMMY1"), Is.EqualTo(TestEnum.BB));
				Assert.That(conn.Execute<TestEnum?>("SELECT 'B' FROM SYSIBM.SYSDUMMY1"), Is.EqualTo(TestEnum.BB));
			}
		}

		[Test, DataContextSource(false)]
		public void TestEnum2(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1", new {p = TestEnum.AA}),
					Is.EqualTo("A"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1", new {p = (TestEnum?) TestEnum.BB}),
					Is.EqualTo("B"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1",
						new {p = ConvertTo<string>.From((TestEnum?) TestEnum.AA)}), Is.EqualTo("A"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1",
						new {p = ConvertTo<string>.From(TestEnum.AA)}), Is.EqualTo("A"));
				Assert.That(
					conn.Execute<string>("SELECT cast(@p as nvarchar(10)) FROM SYSIBM.SYSDUMMY1",
						new {p = conn.MappingSchema.GetConverter<TestEnum?, string>()(TestEnum.AA)}), Is.EqualTo("A"));
			}
		}

		[Table(Name = "ALLTYPES")]
		public class ALLTYPE
		{
			[PrimaryKey, Identity] public int ID { get; set; } // INTEGER
			[Column(DbType = "bigint"), Nullable] public long? BIGINTDATATYPE { get; set; } // BIGINT
			[Column(DbType = "int"), Nullable] public int? INTDATATYPE { get; set; } // INTEGER

			[Column(DbType = "smallint"), Nullable]
			public short? SMALLINTDATATYPE { get; set; } // SMALLINT

			[Column(DbType = "decimal(30)"), Nullable]
			public decimal? DECIMALDATATYPE { get; set; } // DECIMAL

			[Column(DbType = "decfloat(16)"), Nullable]
			public decimal? DECFLOAT16DATATYPE { get; set; } // DECFLOAT16

			[Column(DbType = "decfloat(34)"), Nullable]
			public decimal? DECFLOAT34DATATYPE { get; set; } // DECFLOAT34

			[Column(DbType = "real"), Nullable] public float? REALDATATYPE { get; set; } // REAL
			[Column(DbType = "double"), Nullable] public double? DOUBLEDATATYPE { get; set; } // DOUBLE
			[Column(DbType = "char(1)"), Nullable] public char CHARDATATYPE { get; set; } // CHARACTER

			[Column(DbType = "varchar(20)"), Nullable]
			public string VARCHARDATATYPE { get; set; } // VARCHAR(20)

			[Column(DbType = "clob"), Nullable] public string CLOBDATATYPE { get; set; } // CLOB(1048576)

			[Column(DbType = "dclob(100)"), Nullable]
			public string DBCLOBDATATYPE { get; set; } // DBCLOB(100)

			[Column(DbType = "binary(20)"), Nullable]
			public object BINARYDATATYPE { get; set; } // CHARACTER

			[Column(DbType = "varbinary(20)"), Nullable]
			public object VARBINARYDATATYPE { get; set; } // VARCHAR(5)

			[Column, Nullable] public byte[] BLOBDATATYPE { get; set; } // BLOB(10)

			[Column(DbType = "graphic(10)"), Nullable]
			public string GRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "vargraphic(10)"), Nullable]
			public string VARGRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "date"), Nullable] public DateTime? DATEDATATYPE { get; set; } // DATE
			[Column(DbType = "time"), Nullable] public TimeSpan? TIMEDATATYPE { get; set; } // TIME

			[Column(DbType = "timestamp"), Nullable]
			public DateTime? TIMESTAMPDATATYPE { get; set; } // TIMESTAMP

			[Column, Nullable] public string XMLDATATYPE { get; set; } // XML
		}

		void BulkCopyTest(string context, BulkCopyType bulkCopyType, int maxSize, int batchSize)
		{
			using (var conn = new DataConnection(context))
			{
				//conn.BeginTransaction();
				conn.BulkCopy(
					new BulkCopyOptions
					{
						MaxBatchSize = maxSize,
						BulkCopyType = bulkCopyType,
						NotifyAfter = 10000,
						RowsCopiedCallback = copied => Debug.WriteLine(copied.RowsCopied)
					},
					Enumerable.Range(0, batchSize).Select(n =>
						new ALLTYPE
						{
							ID = 2000 + n,
							BIGINTDATATYPE = 3000 + n,
							INTDATATYPE = 4000 + n,
							SMALLINTDATATYPE = (short) (5000 + n),
							DECIMALDATATYPE = 6000 + n,
							DECFLOAT16DATATYPE = 7000 + n,
							DECFLOAT34DATATYPE = 7000 + n,
							REALDATATYPE = 8000 + n,
							DOUBLEDATATYPE = 9000 + n,
							CHARDATATYPE = 'A',
							VARCHARDATATYPE = "",
							CLOBDATATYPE = null,
							DBCLOBDATATYPE = null,
							BINARYDATATYPE = null,
							VARBINARYDATATYPE = null,
							BLOBDATATYPE = new byte[] {1, 2, 3},
							GRAPHICDATATYPE = "abc",
							VARGRAPHICDATATYPE = "xyz",
							DATEDATATYPE = DateTime.Now.Date,
							TIMEDATATYPE = null,
							TIMESTAMPDATATYPE = null,
							XMLDATATYPE = "<root><element strattr=\"strvalue\" intattr=\"12345\"/></root>"

						}));

				conn.GetTable<ALLTYPE>().Delete(p => p.SMALLINTDATATYPE >= 5000);
			}
		}

		[Test, DataContextSource(false)]
		public void BulkCopyMultipleRows(string context)
		{
			BulkCopyTest(context, BulkCopyType.MultipleRows, 5000, 10001);
		}

		[Test, DataContextSource(false)]
		public void BulkCopyProviderSpecific(string context)
		{
			Assert.Throws<System.NotImplementedException>(delegate
			{
				BulkCopyTest(context, BulkCopyType.ProviderSpecific, 50000, 100001);
			});
		}

		[Test, DataContextSource(false)]
		public void BulkCopyLinqTypesMultipleRows(string context)
		{
			using (var db = new DataConnection(context))
			{
				db.BulkCopy(
					new BulkCopyOptions {BulkCopyType = BulkCopyType.MultipleRows},
					Enumerable.Range(0, 10).Select(n =>
						new LinqDataTypes
						{
							ID = 4000 + n,
							MoneyValue = 1000m + n,
							DateTimeValue = new DateTime(2001, 1, 11, 1, 11, 21, 100),
							BoolValue = true,
							GuidValue = Guid.NewGuid(),
							SmallIntValue = (short) n
						}
					));

				db.GetTable<LinqDataTypes>().Delete(p => p.ID >= 4000);
			}
		}

		[Test, DataContextSource(false)]
		public void TestBinarySize(string context)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					var data = new byte[500000];

					for (var i = 0; i < data.Length; i++)
						data[i] = (byte) (i % byte.MaxValue);

					conn.GetTable<ALLTYPE>().Insert(() => new ALLTYPE
					{
						INTDATATYPE = 2000,
						BLOBDATATYPE = data,
					});

					var blob = conn.GetTable<ALLTYPE>().First(t => t.INTDATATYPE == 2000).BLOBDATATYPE;

					Assert.AreEqual(data, blob);
				}
				finally
				{
					conn.GetTable<ALLTYPE>().Delete(p => p.INTDATATYPE == 2000);
				}
			}
		}

		[Test, DataContextSource(false)]
		public void TestClobSize(string context)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					var sb = new StringBuilder();

					for (var i = 0; i < 100000; i++)
						sb.Append(((char) ((i % (byte.MaxValue - 31)) + 32)).ToString());

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

        [Test, DataContextSource(false)]
		public void TestTypes(string context)
		{
            if (context.Contains("DB2Connect"))
            {
                dynamic int64Value = DB2Types.DB2Int64.CreateInstance(1);
                dynamic int32Value = DB2Types.DB2Int32.CreateInstance(2);
                dynamic int16Value = DB2Types.DB2Int16.CreateInstance(3);

                Assert.That(DB2Types.ConnectionType != null, Is.True);

                Assert.That(int64Value.Value, Is.TypeOf<long>().And.EqualTo(1));
                Assert.That(int32Value.Value, Is.TypeOf<int>().And.EqualTo(2));
                Assert.That(int16Value.Value, Is.TypeOf<short>().And.EqualTo(3));

                dynamic decimalValue = DB2Types.DB2Decimal.CreateInstance(4);
                dynamic decimalValueAsDecimal = DB2Types.DB2DecimalFloat.CreateInstance(5m);
                //dynamic decimalValueAsDouble = DB2Types.DB2DecimalFloat.CreateInstance(6.0);
                //dynamic decimalValueAsLong = DB2Types.DB2DecimalFloat.CreateInstance(7);
                dynamic realValue = DB2Types.DB2Real.CreateInstance(8);
                dynamic real370Value = DB2Types.DB2Real370.CreateInstance(8);
                dynamic doubleValue = DB2Types.DB2Double.CreateInstance(8);
                dynamic stringValue = DB2Types.DB2String.CreateInstance("1");
                dynamic clobValue = DB2Types.DB2Clob.CreateInstance("2");
                dynamic binaryValue = DB2Types.DB2Binary.CreateInstance(new byte[] { 1 });
                dynamic blobValue = DB2Types.DB2Blob.CreateInstance(new byte[] { 2 });
                dynamic dateValue = DB2Types.DB2Date.CreateInstance(new DateTime(2000, 1, 1));
                dynamic timeValue = DB2Types.DB2Time.CreateInstance(new DateTime(1, 1, 1, 1, 1, 1).TimeOfDay);
                dynamic timeStampValue = DB2Types.DB2TimeStamp.CreateInstance(new DateTime(2000, 1, 4));

                Assert.That(decimalValue.Value, Is.TypeOf<decimal>().And.EqualTo(4));
                Assert.That(decimalValueAsDecimal.Value, Is.TypeOf<decimal>().And.EqualTo(5));
                //Assert.That(decimalValueAsDouble.Value, Is.TypeOf<decimal>().And.EqualTo(6));
                //Assert.That(decimalValueAsLong.Value, Is.TypeOf<decimal>().And.EqualTo(7));
                Assert.That(realValue.Value, Is.TypeOf<float>().And.EqualTo(8));
                Assert.That(real370Value.Value, Is.TypeOf<double>().And.EqualTo(8));
                Assert.That(doubleValue.Value, Is.TypeOf<double>().And.EqualTo(8));
                Assert.That(stringValue.Value, Is.TypeOf<string>().And.EqualTo("1"));
                Assert.That(clobValue.Value, Is.TypeOf<string>().And.EqualTo("2"));
                Assert.That(binaryValue.Value, Is.TypeOf<byte[]>().And.EqualTo(new byte[] { 1 }));
                Assert.That(blobValue.Value, Is.TypeOf<byte[]>().And.EqualTo(new byte[] { 2 }));
                Assert.That(dateValue.Value, Is.TypeOf<DateTime>().And.EqualTo(new DateTime(2000, 1, 1)));
                Assert.That(timeValue.Value, Is.TypeOf<TimeSpan>().And.EqualTo(new DateTime(1, 1, 1, 1, 1, 1).TimeOfDay));
                Assert.That(timeStampValue.Value, Is.TypeOf<DateTime>().And.EqualTo(new DateTime(2000, 1, 4)));
            }
            else
            {
                dynamic int64Value = DB2iSeriesTypes.BigInt.CreateInstance(1);
                dynamic int32Value = DB2iSeriesTypes.Integer.CreateInstance(2);
                dynamic int16Value = DB2iSeriesTypes.SmallInt.CreateInstance(3);

                Assert.That(DB2iSeriesTypes.ConnectionType != null, Is.True);

                Assert.That(int64Value.Value, Is.TypeOf<long>().And.EqualTo(1));
                Assert.That(int32Value.Value, Is.TypeOf<int>().And.EqualTo(2));
                Assert.That(int16Value.Value, Is.TypeOf<short>().And.EqualTo(3));

                dynamic decimalValue = DB2iSeriesTypes.Decimal.CreateInstance(4);
                dynamic decimalValueAsDecimal = DB2iSeriesTypes.DecFloat16.CreateInstance(5m);
                dynamic decimalValueAsDouble = DB2iSeriesTypes.DecFloat34.CreateInstance(6.0);
                dynamic decimalValueAsLong = DB2iSeriesTypes.DecFloat34.CreateInstance(7);
                dynamic realValue = DB2iSeriesTypes.Real.CreateInstance(8);
                dynamic stringValue = DB2iSeriesTypes.VarChar.CreateInstance("1");
                dynamic clobValue = DB2iSeriesTypes.Clob.CreateInstance("2");
                dynamic binaryValue = DB2iSeriesTypes.Binary.CreateInstance(new byte[] { 1 });
                dynamic blobValue = DB2iSeriesTypes.Blob.CreateInstance(new byte[] { 2 });
                dynamic dateValue = DB2iSeriesTypes.Date.CreateInstance(new DateTime(2000, 1, 1));
                dynamic timeValue = DB2iSeriesTypes.Time.CreateInstance(new DateTime(1, 1, 1, 1, 1, 1));
                dynamic timeStampValue = DB2iSeriesTypes.TimeStamp.CreateInstance(new DateTime(2000, 1, 4));

                Assert.That(decimalValue.Value, Is.TypeOf<decimal>().And.EqualTo(4));
                Assert.That(decimalValueAsDecimal.Value, Is.TypeOf<decimal>().And.EqualTo(5));
                Assert.That(decimalValueAsDouble.Value, Is.TypeOf<decimal>().And.EqualTo(6));
                Assert.That(decimalValueAsLong.Value, Is.TypeOf<decimal>().And.EqualTo(7));
                Assert.That(realValue.Value, Is.TypeOf<float>().And.EqualTo(8));
                Assert.That(stringValue.Value, Is.TypeOf<string>().And.EqualTo("1"));
                Assert.That(clobValue.Value, Is.TypeOf<string>().And.EqualTo("2"));
                Assert.That(binaryValue.Value, Is.TypeOf<byte[]>().And.EqualTo(new byte[] { 1 }));
                Assert.That(blobValue.Value, Is.TypeOf<byte[]>().And.EqualTo(new byte[] { 2 }));
                Assert.That(dateValue.Value, Is.TypeOf<DateTime>().And.EqualTo(new DateTime(2000, 1, 1)));
                Assert.That(timeValue.Value, Is.TypeOf<DateTime>().And.EqualTo(new DateTime(1, 1, 1, 1, 1, 1)));
                Assert.That(timeStampValue.Value, Is.TypeOf<DateTime>().And.EqualTo(new DateTime(2000, 1, 4)));


                int64Value = DB2iSeriesTypes.BigInt.CreateInstance();
                int32Value = DB2iSeriesTypes.Integer.CreateInstance();
                int16Value = DB2iSeriesTypes.SmallInt.CreateInstance();

                Assert.That(int64Value.IsNull, Is.True);
                Assert.That(int32Value.IsNull, Is.True);
                Assert.That(int16Value.IsNull, Is.True);

                Assert.That(((dynamic)DB2iSeriesTypes.Decimal.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.DecFloat16.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.DecFloat34.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.Real.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.VarChar.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.Binary.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.Date.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.Time.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.TimeStamp.CreateInstance()).IsNull, Is.True);
                Assert.That(((dynamic)DB2iSeriesTypes.RowId.CreateInstance()).IsNull, Is.True);
            }
		}

		[Test, DataContextSource(false)]
		public void TestAny(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>();

				Assert.True(person.Any(p => p.ID == 2));
				Assert.False(person.Any(p => p.ID == 23));
				Assert.True(person.Any(p => !(p.ID == 23)));
			}
		}

		[Test, DataContextSource(false)]
		public void TestOrderBySkipTake(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>().OrderBy(p => p.LastName).Skip(2).Take(2);

				var results = person.ToArray();

				Assert.AreEqual(2, results.Count());
				Assert.AreEqual("Peacock", results.First().LastName);
				Assert.AreEqual("Plum", results.Last().LastName);
			}
		}

		[Test, DataContextSource(false)]
		public void TestOrderByDescendingSkipTake(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>().OrderByDescending(p => p.LastName).Skip(2).Take(2);

				var results = person.ToArray();

				Assert.AreEqual(2, results.Count());
				Assert.AreEqual("Scarlet", results.First().LastName);
				Assert.AreEqual("Pupkin", results.Last().LastName);
			}
		}

		[Test, DataContextSource(false)]
		public void CompareDate1(string context)
		{
			using (var db = GetDataContext(context))
			{
				var expected = Types.Where(t => t.ID == 1 && t.DateTimeValue <= DateTime.Today);

				var actual = db.Types.Where(t => t.ID == 1 && t.DateTimeValue <= DateTime.Today);

				Assert.AreEqual(expected, actual);
			}
		}

		[Test, DataContextSource(false)]
		public void CompareDate2(string context)
		{
			var dt = Types2[3].DateTimeValue;

			using (var db = GetDataContext(context))
			{
				var expected = Types2.Where(t => t.DateTimeValue.Value.Date > dt.Value.Date);
				var actual = db.Types2.Where(t => t.DateTimeValue.Value.Date > dt.Value.Date);

				AreEqual(expected, actual);
			}
		}

	    [Table("InsertOrUpdateByte")]
	    class MergeTypesByte
	    {
	        [Column("Id", IsIdentity = true)] [PrimaryKey] public int Id { get; set; }

	        [Column("FieldByteAsDecimal", DataType = DataType.Decimal, Length = 2, Precision = 0)] public byte FieldByte { get; set; }

	        [Column("FieldULongAsDecimal", DataType = DataType.Decimal, Length = 20, Precision = 0)] public ulong FieldULong { get; set; }
        }

	    [Test, DataContextSource(false)]
	    public void InsertOrUpdateWithIntegers(string context)
	    {
	        using (var db = new TestDataConnection(context))
	        {
	            LinqToDB.ITable<MergeTypesByte> table;
	            using (new DisableLogging())
	            {
	                db.DropTable<MergeTypesByte>(throwExceptionIfNotExists: false);
	                table = db.CreateTable<MergeTypesByte>();
	            }

	            ulong val = long.MaxValue;

                table.InsertOrUpdate(
	                () => new MergeTypesByte { FieldByte = 27, FieldULong = val },
	                s => new MergeTypesByte { FieldByte = 27, FieldULong = val },
	                () => new MergeTypesByte { FieldByte = 22, FieldULong = val }
	            );

	            Assert.AreEqual(1, table.Count());
	        }
	    }
    }
}
