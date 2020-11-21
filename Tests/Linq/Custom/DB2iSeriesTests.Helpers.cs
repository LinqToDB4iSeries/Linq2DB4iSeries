using LinqToDB;
using LinqToDB.Data;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq;

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		protected string GetNullSql = "SELECT {0} FROM {1} WHERE ID = 1";
		protected string GetValueSql = "SELECT {0} FROM {1} WHERE ID = 2";
		protected string PassNullSql = "SELECT ID FROM {1} WHERE {2} IS NULL AND {0} IS NULL OR {3} IS NOT NULL AND {0} = {4}";
		protected string PassValueSql = "SELECT ID FROM {1} WHERE {0} = {2}";

		protected T TestType<T>(DataConnection conn, string fieldName,
			DataType dataType = DataType.Undefined,
			string tableName = "AllTypes",
			string castTo = null!,
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

		static void TestNumeric<T>(DataConnection conn, T expectedValue, DataType dataType, string skip = "")
		{
			var skipTypes = skip.Split(' ').ToList();

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
				var sqlValue = expectedValue is bool ? (bool)(object)expectedValue ? 1 : 0 : (object)expectedValue!;
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
	}
}
