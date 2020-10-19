using System;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	/// <summary>
	/// Wrapper class of OdbcDataReader to intercept issues with IBM driver
	/// </summary>
	internal class OdbcDataReaderWrapper : DbDataReader
	{
		private readonly DbDataReader odbcDataReader;

		public OdbcDataReaderWrapper(DbDataReader odbcDataReader)
		{
			this.odbcDataReader = odbcDataReader;
		}

		/// <summary>
		/// Handles IBM driver throwing exception on XML data type columns
		/// </summary>
		/// <param name="ordinal"></param>
		/// <returns></returns>
		public override Type GetFieldType(int ordinal)
		{
			if (GetDataTypeName(ordinal) == "XML")
				return typeof(string);
			else
				return odbcDataReader.GetFieldType(ordinal);
		}

		/// <summary>
		/// Handles IBM driver throwing exception on XML data type columns
		/// </summary>
		/// <param name="ordinal"></param>
		/// <returns></returns>
		public override bool IsDBNull(int ordinal) 
		{
			if (GetDataTypeName(ordinal) == "XML")
				return GetValue(ordinal) == DBNull.Value;
			else
				return odbcDataReader.IsDBNull(ordinal);
		}

		public override DataTable GetSchemaTable() => odbcDataReader.GetSchemaTable();
		
		public override object this[int ordinal] => odbcDataReader[ordinal];

		public override object this[string name] => odbcDataReader[name];

		public override int Depth => odbcDataReader.Depth;

		public override int FieldCount => odbcDataReader.FieldCount;

		public override bool HasRows => odbcDataReader.HasRows;

		public override bool IsClosed => odbcDataReader.IsClosed;

		public override int RecordsAffected => odbcDataReader.RecordsAffected;

		public override void Close() => odbcDataReader.Close();

		public override bool GetBoolean(int ordinal) => odbcDataReader.GetBoolean(ordinal);

		public override byte GetByte(int ordinal) => odbcDataReader.GetByte(ordinal);

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
			=> odbcDataReader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

		public override char GetChar(int ordinal) => odbcDataReader.GetChar(ordinal);

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
			=> odbcDataReader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

		public override string GetDataTypeName(int ordinal) => odbcDataReader.GetDataTypeName(ordinal);

		public override DateTime GetDateTime(int ordinal) => odbcDataReader.GetDateTime(ordinal);

		public override decimal GetDecimal(int ordinal) => odbcDataReader.GetDecimal(ordinal);
		
		public override double GetDouble(int ordinal) => odbcDataReader.GetDouble(ordinal);
		
		public override IEnumerator GetEnumerator() => odbcDataReader.GetEnumerator();
		
		public override float GetFloat(int ordinal) => odbcDataReader.GetFloat(ordinal);
		
		public override Guid GetGuid(int ordinal) => odbcDataReader.GetGuid(ordinal);
		
		public override short GetInt16(int ordinal) => odbcDataReader.GetInt16(ordinal);
		
		public override int GetInt32(int ordinal) => odbcDataReader.GetInt32(ordinal);
		
		public override long GetInt64(int ordinal) => odbcDataReader.GetInt64(ordinal);
		
		public override string GetName(int ordinal) => odbcDataReader.GetName(ordinal);
		
		public override int GetOrdinal(string name) => odbcDataReader.GetOrdinal(name);
		
		public override string GetString(int ordinal) => odbcDataReader.GetString(ordinal);
		
		public override object GetValue(int ordinal) => odbcDataReader.GetValue(ordinal);
		
		public override int GetValues(object[] values) => odbcDataReader.GetValues(values);
		
		public override bool NextResult() => odbcDataReader.NextResult();
		
		public override bool Read() => odbcDataReader.Read();
	}
}
