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
using LinqToDB.DataProvider.DB2iSeries;
using LinqToDB.Mapping;
using NUnit.Framework;

#if NETFRAMEWORK
using IBM.Data.DB2.iSeries;
#endif

namespace Tests.DataProvider
{
	using Model;

	static class DB2iSeriesTestQueryExtensions
	{
		public static T ExecuteScalar<T>(this DataConnection connection, string value, string? castTo = null)
			=> connection.Execute<T>(GetScalarQuery(value, castTo));


		public static T ExecuteScalarParameter<T>(this DataConnection connection, string parameterName, string parameterType, object parameterValue, DataType? dataType = null)
		{
			var parameter = new DataParameter(parameterName, parameterValue);

			return connection.ExecuteScalarParameter<T>(parameter, parameterType, dataType);
		}

		public static T ExecuteScalarParameter<T>(this DataConnection connection, DataParameter dataParameter, string parameterType, DataType? dataType = null)
		{
			if (connection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider
				&& (iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.Odbc
					|| iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.OleDb))
			{
				dataParameter.Name = "?";
			}

			if (dataType.HasValue)
				dataParameter.DataType = dataType.Value;

			return connection.Execute<T>(GetScalarParameterQuery(dataParameter.Name, parameterType), dataParameter);
		}

		public static T ExecuteScalarParameterObject<T>(this DataConnection connection, string parameterName, string parameterType, object parameterValuesObject)
		{
			if (connection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider
				&& (iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.Odbc
					|| iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.OleDb))
			{
				parameterName = "?";
			}

			return connection.Execute<T>(GetScalarParameterQuery(parameterName, parameterType), parameterValuesObject);
		}

		public static T ExecuteScalarParameterObject<T>(this DataConnection connection, string expression, object parameterValuesObject)
		{
			return connection.Execute<T>(GetScalarQuery(expression), parameterValuesObject);
		}

		private static string GetScalarQuery(string value, string? castTo = null)
		{
			var sb = new StringBuilder().Append("SELECT ");
			if (!string.IsNullOrEmpty(castTo))
				sb.Append("CAST(");
			sb.Append(value);
			if (!string.IsNullOrEmpty(castTo))
				sb.Append(" AS ").Append(castTo).Append(")");
			sb.Append(" FROM SYSIBM.SYSDUMMY1");
			return sb.ToString();
		}

		private static string GetScalarParameterQuery(string parameterName, string parameterType)
		{
			var sb = new StringBuilder()
				.Append("SELECT ")
				.Append("CAST(")
				.Append(parameterName == "?" ? "" : "@").Append(parameterName)
				.Append(" AS ").Append(parameterType).Append(")")
				.Append("FROM SYSIBM.SYSDUMMY1");
			return sb.ToString();
		}

		public static string AsQuoted(this string s) => $"'{s}'";

		public static string GetParameterMarker(this DataConnection dataConnection, string parameterName, string? castTo = null)
		{
			return GetValueSql(
				dataConnection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider
				&& (iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.Odbc
					|| iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.OleDb)
				? "?" : parameterName, castTo);
		}

		public static string GetValueSql(string expression, string? castTo = null)
		{
			var sb = new StringBuilder();
			if (!string.IsNullOrEmpty(castTo))
				sb.Append("CAST(");

			sb.Append(expression == "?" ? "" : "@").Append(expression);

			if (!string.IsNullOrEmpty(castTo))
				sb.Append(" AS ").Append(castTo).Append(")");
			return sb.ToString();
		}
	}

	[TestFixture]
	public class DB2iSeriesTests : TestBase
	{
		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestParameters(string context)
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

		protected string GetNullSql = "SELECT {0} FROM {1} WHERE ID = 1";
		protected string GetValueSql = "SELECT {0} FROM {1} WHERE ID = 2";
		protected string PassNullSql = "SELECT ID FROM {1} WHERE {2} IS NULL AND {0} IS NULL OR {3} IS NOT NULL AND {0} = {4}";
		protected string PassValueSql = "SELECT ID FROM {1} WHERE {0} = {2}";

		protected T TestType<T>(DataConnection conn, string fieldName,
			DataType dataType = DataType.Undefined,
			string tableName = "AllTypes",
			string? castTo = null,
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
			Debug.WriteLine("{0} {1}:{2} -> NULL", fieldName, (object)type.Name, dataType);

			var sql = string.Format(GetNullSql, fieldName, tableName);
			var value = conn.Execute<T>(sql);
			var def = conn.MappingSchema.GetDefaultValue(typeof(T));
			Assert.That(value, Is.EqualTo(def));

			int? id;

			if (!skipNull && !skipPass && PassNullSql != null)
			{
				sql = string.Format(PassNullSql, fieldName, tableName, conn.GetParameterMarker("p", castTo), conn.GetParameterMarker("p1", castTo), conn.GetParameterMarker("p2", castTo));

				if (!skipDefinedNull && dataType != DataType.Undefined)
				{
					// Get NULL ID with dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> NULL ID with dataType", fieldName, (object)type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value, dataType), new DataParameter("p1", value, dataType),
						new DataParameter("p2", value, dataType));
					Assert.That(id, Is.EqualTo(1));
				}

				if (!skipDefaultNull)
				{
					// Get NULL ID with default dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> NULL ID with default dataType", fieldName, (object)type.Name, dataType);
					id = conn.Execute<int?>(sql, new { p = value, p1 = value, p2 = value });
					Assert.That(id, Is.EqualTo(1));
				}

				if (!skipUndefinedNull)
				{
					// Get NULL ID without dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> NULL ID without dataType", fieldName, (object)type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value, dataType), new DataParameter("p1", value, dataType),
						new DataParameter("p2", value, dataType));
					Assert.That(id, Is.EqualTo(1));
				}
			}

			// Get value.
			//
			Debug.WriteLine("{0} {1}:{2} -> value", fieldName, (object)type.Name, dataType);
			sql = string.Format(GetValueSql, fieldName, tableName);
			value = conn.Execute<T>(sql);

			if (!skipNotNull && !skipPass)
			{
				sql = string.Format(PassValueSql, fieldName, tableName, conn.GetParameterMarker("p", castTo));

				if (!skipDefined && dataType != DataType.Undefined)
				{
					// Get value ID with dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> value ID with dataType", fieldName, (object)type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value, dataType));
					Assert.That(id, Is.EqualTo(2));
				}

				if (!skipDefault)
				{
					// Get value ID with default dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> value ID with default dataType", fieldName, (object)type.Name, dataType);
					id = conn.Execute<int?>(sql, new { p = value });
					Assert.That(id, Is.EqualTo(2));
				}

				if (!skipUndefined)
				{
					// Get value ID without dataType.
					//
					Debug.WriteLine("{0} {1}:{2} -> value ID without dataType", fieldName, (object)type.Name, dataType);
					id = conn.Execute<int?>(sql, new DataParameter("p", value));
					Assert.That(id, Is.EqualTo(2));
				}
			}

			return value;
		}


		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		//DecFloatTests break on AccessClient with cultures that have a different decimal point than period.
		[SetCulture("en-US")]
		public void TestDataTypes(string context)
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
				if (!context.ToUpper().Contains("ODBC"))
				{
					Assert.That(TestType<string>(conn, "xmlDataType", DataType.Xml, skipPass: true),
						Is.EqualTo("<root><element strattr=\"strvalue\" intattr=\"12345\"/></root>"));
				}
			}
		}

#if NETFRAMEWORK
		[Test, IncludeDataContextSource(TestProvName.DB2iNet)]
		//DecFloatTests break on AccessClient with cultures that have a different decimal point than period.
		[SetCulture("en-US")]
		public void TestDataTypes_AccessClient(string context)
		{
			using (var conn = new DataConnection(context))
			{
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
				Assert.That(TestType<iDB2DecFloat16?>(conn, "decfloat16DataType", DataType.Decimal), Is.EqualTo(new iDB2DecFloat16(888.456m)));
				Assert.That(TestType<iDB2DecFloat34?>(conn, "decfloat34DataType", DataType.Decimal).ToString(), Is.EqualTo(new iDB2DecFloat34(777.987m).ToString()));
			}
		}
#endif
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
				var sqlValue = expectedValue is bool ? (bool)(object)expectedValue ? 1 : 0 : (object)expectedValue;
				var sql = string.Format("VALUES Cast({0} as {1})", sqlValue ?? "NULL", sqlType);
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
					castType = "decimal(20, 0)";
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
						castType = string.Format("decimal({0}, {1})", precision, scale);
					}
					else
						castType = "decimal";

					break;
				case DataType.Money:
					castType = "decfloat";
					break;
			}

			var parameterName = "p";
			var parameterMarker = conn.GetParameterMarker(parameterName);

			Debug.WriteLine("{0} -> DataType.{1}", typeof(T), dataType);
			string sql1 = $"VALUES cast({parameterMarker} as {castType})";

			Assert.That(conn.Execute<T>(sql1, new DataParameter { Name = parameterName, DataType = dataType, Value = expectedValue }),
				Is.EqualTo(expectedValue));
			Debug.WriteLine("{0} -> auto", typeof(T));
			Assert.That(
				conn.Execute<T>($"VALUES cast({parameterMarker} as {castType})",
					new DataParameter { Name = parameterName, Value = expectedValue }), Is.EqualTo(expectedValue));
			Debug.WriteLine("{0} -> new", typeof(T));
			Assert.That(conn.Execute<T>($"VALUES cast({parameterMarker} as {castType})", new { p = expectedValue }),
				Is.EqualTo(expectedValue));
		}

		static void TestSimple<T>(DataConnection conn, T expectedValue, DataType dataType)
			where T : struct
		{
			TestNumeric<T>(conn, expectedValue, dataType);
			TestNumeric<T?>(conn, expectedValue, dataType);
			TestNumeric<T?>(conn, (T?)null, dataType);
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		//Test uses string format to build sql values, invariant culture is needed
		[SetCulture("en-US")]
		public void TestNumerics(string context)
		{
			var skipDecFloat = TestProvName.IsiSeriesOleDb(context) ? " decfloat" : "";

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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb)]
		public void TestDate(string context)
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

				//iSeries native provider cannot assign datetime parameter to date
				if (context != TestProvName.DB2iNet)
				{
					Assert.That(
					conn.ExecuteScalarParameter<DateTime>("p", "date", dateTime), Is.EqualTo(dateTime));
				}
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestDateTime(string context)
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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestTimeSpan(string context)
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



		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestChar(string context)
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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestString(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var asciiText = "123ab"; var quotedAsciiText = asciiText.AsQuoted();
				var unicodeText = "αβγδε"; var quotedUnicodeText = unicodeText.AsQuoted();

				Assert.That(conn.ExecuteScalar<string>(quotedAsciiText, "char(5)"), Is.EqualTo(asciiText));
				Assert.That(conn.ExecuteScalar<string>(quotedAsciiText, "char(20)"), Is.EqualTo(asciiText));
				Assert.That(conn.ExecuteScalar<string>("NULL", "char(5)"), Is.Null);

				Assert.That(conn.ExecuteScalar<string>(quotedAsciiText, "varchar(5)"), Is.EqualTo(asciiText));
				Assert.That(conn.ExecuteScalar<string>(quotedAsciiText, "varchar(20)"), Is.EqualTo(asciiText));
				Assert.That(conn.ExecuteScalar<string>("NULL", "varchar(5)"), Is.Null);

				Assert.That(conn.ExecuteScalar<string>(quotedAsciiText, "clob"), Is.EqualTo(asciiText));
				Assert.That(conn.ExecuteScalar<string>("NULL", "clob"), Is.Null);

				Assert.That(conn.ExecuteScalar<string>(quotedUnicodeText, "nchar(5)"), Is.EqualTo(unicodeText));
				//Assert.That(conn.ExecuteScalar<string>(quotedUnicodeText, "nchar(20)"), Is.EqualTo(unicodeText));
				Assert.That(conn.ExecuteScalar<string>("NULL", "nchar(5)"), Is.Null);

				Assert.That(conn.ExecuteScalar<string>(quotedUnicodeText, "nvarchar(5)"), Is.EqualTo(unicodeText));
				Assert.That(conn.ExecuteScalar<string>(quotedUnicodeText, "nvarchar(20)"), Is.EqualTo(unicodeText));
				Assert.That(conn.ExecuteScalar<string>("NULL", "nvarchar(5)"), Is.Null);

				Assert.That(conn.ExecuteScalar<string>(quotedUnicodeText, "nclob"), Is.EqualTo(unicodeText));
				Assert.That(conn.ExecuteScalar<string>("NULL", "nclob"), Is.Null);

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
					conn.ExecuteScalarParameter<string>(DataParameter.Create("p", (string?)null), "nvarchar(10)"),
					Is.Null);

				//This case fails on ODBC provider. Casting a numeric parameter to nvarchar/nclob produces null
				//Assert.That(
				//	conn.ExecuteScalarParameter<string>("p", "nvarchar(10)", 1),
				//	Is.EqualTo("1"));
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestBinary(string context)
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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet73, TestProvName.DB2iNet, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestGuidBlob(string context)
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


		[Test, IncludeDataContextSource(TestProvName.DB2iNet73GAS, TestProvName.DB2iNetGAS)]
		public void TestGuidAsString(string context)
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


		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestXml(string context)
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

		enum TestEnum
		{
			[MapValue("A")] AA,
			[MapValue("B")] BB,
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestEnum1(string context)
		{
			using (var conn = new DataConnection(context))
			{
				Assert.That(conn.ExecuteScalar<TestEnum>("'A'"), Is.EqualTo(TestEnum.AA));
				Assert.That(conn.ExecuteScalar<TestEnum?>("'A'"), Is.EqualTo(TestEnum.AA));
				Assert.That(conn.ExecuteScalar<TestEnum>("'B'"), Is.EqualTo(TestEnum.BB));
				Assert.That(conn.ExecuteScalar<TestEnum?>("'B'"), Is.EqualTo(TestEnum.BB));
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestEnum2(string context)
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

		[Table(Name = "ALLTYPES")]
		public class ALLTYPE
		{
			[PrimaryKey, Identity]
			public int ID { get; set; } // INTEGER

			[Column(DbType = "bigint"), Nullable]
			public long? BIGINTDATATYPE { get; set; } // BIGINT

			[Column(DbType = "int"), Nullable]
			public int? INTDATATYPE { get; set; } // INTEGER

			[Column(DbType = "smallint"), Nullable]
			public short? SMALLINTDATATYPE { get; set; } // SMALLINT

			[Column(DbType = "decimal(30)"), Nullable]
			public decimal? DECIMALDATATYPE { get; set; } // DECIMAL

			[Column(DbType = "decfloat(16)"), Nullable]
			public decimal? DECFLOAT16DATATYPE { get; set; } // DECFLOAT16

			[Column(DbType = "decfloat(34)"), Nullable]
			public decimal? DECFLOAT34DATATYPE { get; set; } // DECFLOAT34

			[Column(DbType = "real"), Nullable]
			public float? REALDATATYPE { get; set; } // REAL

			[Column(DbType = "double"), Nullable]
			public double? DOUBLEDATATYPE { get; set; } // DOUBLE

			[Column(DbType = "char(1)"), Nullable]
			public char CHARDATATYPE { get; set; } // CHARACTER

			[Column(DbType = "varchar(20)"), Nullable]
			public string VARCHARDATATYPE { get; set; } // VARCHAR(20)

			[Column(DbType = "clob"), Nullable]
			public string CLOBDATATYPE { get; set; } // CLOB(1048576)

			[Column(DbType = "dbclob(100)"), Nullable]
			public string DBCLOBDATATYPE { get; set; } // DBCLOB(100)

			[Column(DbType = "binary(20)"), Nullable]
			public object BINARYDATATYPE { get; set; } // BINARY(20)

			[Column(DbType = "varbinary(20)"), Nullable]
			public object VARBINARYDATATYPE { get; set; } // VARBINARY(20)

			[Column(DbType = "blob"), Nullable]
			public byte[] BLOBDATATYPE { get; set; } // BLOB(10)

			[Column(DbType = "graphic(10)"), Nullable]
			public string GRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "vargraphic(10)"), Nullable]
			public string VARGRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "date"), Nullable]
			public DateTime? DATEDATATYPE { get; set; } // DATE

			[Column(DbType = "time"), Nullable]
			public TimeSpan? TIMEDATATYPE { get; set; } // TIME

			[Column(DbType = "timestamp"), Nullable]
			public DateTime? TIMESTAMPDATATYPE { get; set; } // TIMESTAMP

			[Column, Nullable]
			public string XMLDATATYPE { get; set; } // XML
		}

		[Table(Name = "ALLTYPES2")]
		public class ALLTYPE2
		{
			[PrimaryKey, Identity]
			public int ID { get; set; } // INTEGER

			[Column(DbType = "bigint"), Nullable]
			public long? BIGINTDATATYPE { get; set; } // BIGINT

			[Column(DbType = "int"), Nullable]
			public int? INTDATATYPE { get; set; } // INTEGER

			[Column(DbType = "smallint"), Nullable]
			public short? SMALLINTDATATYPE { get; set; } // SMALLINT

			[Column(DbType = "decimal(30)"), Nullable]
			public decimal? DECIMALDATATYPE { get; set; } // DECIMAL

			[Column(DbType = "decfloat(16)"), Nullable]
			public decimal? DECFLOAT16DATATYPE { get; set; } // DECFLOAT16

			[Column(DbType = "decfloat(34)"), Nullable]
			public decimal? DECFLOAT34DATATYPE { get; set; } // DECFLOAT34

			[Column(DbType = "real"), Nullable]
			public float? REALDATATYPE { get; set; } // REAL

			[Column(DbType = "double"), Nullable]
			public double? DOUBLEDATATYPE { get; set; } // DOUBLE

			[Column(DbType = "char(1)"), Nullable]
			public char CHARDATATYPE { get; set; } // CHARACTER

			[Column(DbType = "varchar(20)"), Nullable]
			public string VARCHARDATATYPE { get; set; } // VARCHAR(20)

			[Column(DbType = "graphic(10)"), Nullable]
			public string GRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "vargraphic(10)"), Nullable]
			public string VARGRAPHICDATATYPE { get; set; } // GRAPHIC(10)
			
			[Column(DbType = "binary(20)"), Nullable]
			public object BINARYDATATYPE { get; set; } // BINARY(20)

			[Column(DbType = "varbinary(20)"), Nullable]
			public object VARBINARYDATATYPE { get; set; } // VARBINARY(20)

			[Column(DbType = "date"), Nullable]
			public DateTime? DATEDATATYPE { get; set; } // DATE

			[Column(DbType = "time"), Nullable]
			public TimeSpan? TIMEDATATYPE { get; set; } // TIME

			[Column(DbType = "timestamp"), Nullable]
			public DateTime? TIMESTAMPDATATYPE { get; set; } // TIMESTAMP
		}

		void BulkCopyTest(string context, BulkCopyType bulkCopyType, int maxSize, int batchSize)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
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
								SMALLINTDATATYPE = (short)(5000 + n),
								DECIMALDATATYPE = 6000 + n,
								DECFLOAT16DATATYPE = 7000 + n,
								DECFLOAT34DATATYPE = 7000 + n,
								REALDATATYPE = 8000 + n,
								DOUBLEDATATYPE = 9000 + n,
								CHARDATATYPE = 'A',
								VARCHARDATATYPE = "",
								CLOBDATATYPE = "123",
								DBCLOBDATATYPE = "αβγ",
								BINARYDATATYPE = new byte[] { 1, 2, 3 },
								VARBINARYDATATYPE = new byte[] { 1, 2, 3 },
								BLOBDATATYPE = new byte[] { 1, 2, 3 },
								GRAPHICDATATYPE = "αβγ",
								VARGRAPHICDATATYPE = "βγδ",
								DATEDATATYPE = DateTime.Now.Date,
								TIMEDATATYPE = TimeSpan.FromSeconds(10),
								TIMESTAMPDATATYPE = DateTime.Now,
								XMLDATATYPE = "<root><element strattr=\"strvalue\" intattr=\"12345\"/></root>"
							}));
				}
				catch (Exception e)
				{
					Assert.Fail(e.Message);
				}
				finally
				{
					conn.GetTable<ALLTYPE>().Delete(p => p.DECIMALDATATYPE >= 6000);
				}
			}
		}

		void BulkCopyTest2(string context, BulkCopyType bulkCopyType, int maxSize, int batchSize)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					conn.BulkCopy(
						new BulkCopyOptions
						{
							MaxBatchSize = maxSize,
							BulkCopyType = bulkCopyType,
							NotifyAfter = 10000,
							RowsCopiedCallback = copied => Debug.WriteLine(copied.RowsCopied)
						},
						Enumerable.Range(0, batchSize).Select(n =>
							new ALLTYPE2
							{
								ID = 2000 + n,
								BIGINTDATATYPE = 3000 + n,
								INTDATATYPE = 4000 + n,
								SMALLINTDATATYPE = (short)(5000 + n),
								DECIMALDATATYPE = 6000 + n,
								DECFLOAT16DATATYPE = 7000 + n,
								DECFLOAT34DATATYPE = 7000 + n,
								REALDATATYPE = 8000 + n,
								DOUBLEDATATYPE = 9000 + n,
								CHARDATATYPE = 'A',
								VARCHARDATATYPE = "123",
								BINARYDATATYPE = new byte[] { 1, 2, 3 },
								VARBINARYDATATYPE = new byte[] { 1, 2, 3 },
								GRAPHICDATATYPE = "αβγ",
								VARGRAPHICDATATYPE = "βγδ",
								DATEDATATYPE = DateTime.Now.Date,
								TIMEDATATYPE = TimeSpan.FromSeconds(10),
								TIMESTAMPDATATYPE = DateTime.Now,
							}));
				}
				catch (Exception e)
				{
					Assert.Fail(e.Message);
				}
				finally
				{
					conn.GetTable<ALLTYPE>().Delete(p => p.DECIMALDATATYPE >= 6000);
				}
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void BulkCopyMultipleRows(string context)
		{
			BulkCopyTest(context, BulkCopyType.MultipleRows, 5000, 100);
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iDB2Connect)]
		public void BulkCopyProviderSpecific(string context)
		{
			if (TestProvName.IsiSeriesAccessClient(context))
				BulkCopyTest2(context, BulkCopyType.ProviderSpecific, 50000, 100001);
			if (TestProvName.IsiSeriesDB2Connect(context))
				BulkCopyTest(context, BulkCopyType.ProviderSpecific, 50000, 100001);
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void BulkCopyLinqTypesMultipleRows(string context)
		{
			using (var db = new DataConnection(context))
			{
				try
				{
					db.BulkCopy(
						new BulkCopyOptions { BulkCopyType = BulkCopyType.MultipleRows },
						Enumerable.Range(0, 10).Select(n =>
							new LinqDataTypes
							{
								ID = 4000 + n,
								MoneyValue = 1000m + n,
								DateTimeValue = new DateTime(2001, 1, 11, 1, 11, 21, 100),
								BoolValue = true,
								GuidValue = Guid.NewGuid(),
								SmallIntValue = (short)n
							}
						));
				}
				catch (Exception e)
				{
					Assert.Fail(e.ToString());
				}
				finally
				{
					db.GetTable<LinqDataTypes>().Delete(p => p.ID >= 4000);
				}
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestBinarySize(string context)
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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestClobSize(string context)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					var sb = new StringBuilder();

					for (var i = 0; i < 100000; i++)
						sb.Append(((char)((i % (byte.MaxValue - 31)) + 32)).ToString());

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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iNet73, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestOrderBySkipTake(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>().OrderBy(p => p.LastName).Skip(2).Take(2);

				var results = person.ToArray();

				Assert.AreEqual(2, results.Count());
				Assert.AreEqual("Pupkin", results.First().LastName);
				Assert.AreEqual("Testerson", results.Last().LastName);
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iNet73, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void TestOrderByDescendingSkipTake(string context)
		{
			using (var conn = new DataConnection(context))
			{
				var person = conn.GetTable<Person>().OrderByDescending(p => p.LastName).Skip(2).Take(2);

				var results = person.ToArray();

				Assert.AreEqual(2, results.Count());
				Assert.AreEqual("König", results.First().LastName);
				Assert.AreEqual("Doe", results.Last().LastName);
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
		public void CompareDate1(string context)
		{
			using (var db = GetDataContext(context))
			{
				var expected = Types.Where(t => t.ID == 1 && t.DateTimeValue <= DateTime.Today);

				var actual = db.Types.Where(t => t.ID == 1 && t.DateTimeValue <= DateTime.Today);

				Assert.AreEqual(expected, actual);
			}
		}

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
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

		[Test, IncludeDataContextSource(TestProvName.DB2iNet, TestProvName.DB2iODBC, TestProvName.DB2iOleDb, TestProvName.DB2iDB2Connect)]
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
