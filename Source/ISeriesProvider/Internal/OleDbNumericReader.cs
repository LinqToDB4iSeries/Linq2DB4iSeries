using System;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using LinqToDB.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	// System.Data.OleDb reads DBTYPE_NUMERIC (DbBuffer.ReadNumeric) by copying the 128-bit coefficient
	// of the DB_NUMERIC structure as is and throws when it does not fit 96 bits, without removing trailing
	// zeros first. Set operations can widen the scale enough to overflow even when the value fits decimal,
	// e.g. DECIMAL(18, 2) UNION DECIMAL(18, 17) yields DECIMAL(33, 17).
	// No public OleDbDataReader API exposes a DBTYPE_NUMERIC column other than through ReadNumeric, so on
	// overflow the DB_NUMERIC structure is read from the internal row buffer and normalized. Values that
	// still do not fit decimal after removing trailing zeros keep the original exception.
	internal static class OleDbNumericReader
	{
		const short DBTYPE_NUMERIC = 131;
		// precision, scale, sign, val[16]
		const int DbNumericSize = 19;
		const int MaxDecimalScale = 28;

		static RawReader? rawReader;

		public static decimal GetDecimal(DbDataReader reader, int ordinal)
		{
			try
			{
				return reader.GetDecimal(ordinal);
			}
			catch (InvalidCastException) when (TryReadNumeric(reader, ordinal, out var value))
			{
				return value;
			}
		}

		static bool TryReadNumeric(DbDataReader reader, int ordinal, out decimal value)
		{
			value = default;

			if (rawReader == null)
			{
				if (reader.GetType().FullName != "System.Data.OleDb.OleDbDataReader")
					return false;

				rawReader = new RawReader(reader.GetType());
			}
			else if (reader.GetType() != rawReader.ReaderType)
			{
				return false;
			}

			var numeric = rawReader.Read?.Invoke(reader, ordinal);

			return numeric != null && TryDecode(numeric, out value);
		}

		static bool TryDecode(byte[] numeric, out decimal value)
		{
			value = default;

			int scale     = numeric[1];
			var negative  = numeric[2] == 0;
			var magnitude = new uint[4];
			for (var i = 0; i < magnitude.Length; i++)
				magnitude[i] = BitConverter.ToUInt32(numeric, 3 + i * 4);

			var quotient = new uint[4];
			while (scale > 0 && DivRem10(magnitude, quotient) == 0)
			{
				Array.Copy(quotient, magnitude, magnitude.Length);
				scale--;
			}

			if (magnitude[3] != 0 || scale > MaxDecimalScale)
				return false;

			value = new decimal((int)magnitude[0], (int)magnitude[1], (int)magnitude[2], negative, (byte)scale);
			return true;
		}

		static uint DivRem10(uint[] dividend, uint[] quotient)
		{
			ulong remainder = 0;
			for (var i = dividend.Length - 1; i >= 0; i--)
			{
				var current = (remainder << 32) | dividend[i];
				quotient[i] = (uint)(current / 10);
				remainder   = current % 10;
			}

			return (uint)remainder;
		}

		// Reads the raw DB_NUMERIC bytes of a column through internal members of System.Data.OleDb
		// (System.Data on .NET Framework). Read is null when the reader type does not expose them.
		sealed class RawReader
		{
			const BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

			public RawReader(Type readerType)
			{
				ReaderType = readerType;

				// r => (binding = ((OleDbDataReader)r).GetColumnBinding(i)).DbType == DBTYPE_NUMERIC
				//     ? binding.RowBinding.ReadBytes(binding.ValueOffset, DbNumericSize)
				//     : null
				var getColumnBinding = readerType.GetMethod("GetColumnBinding", Flags, null, [typeof(int)], null);
				var bindingType      = getColumnBinding?.ReturnType;
				var dbType           = bindingType?.GetProperty("DbType", Flags);
				var rowBinding       = bindingType?.GetProperty("RowBinding", Flags);
				var valueOffset      = bindingType?.GetProperty("ValueOffset", Flags);
				var readBytes        = rowBinding?.PropertyType.GetMethod("ReadBytes", Flags, null, [typeof(int), typeof(int)], null);

				if (getColumnBinding == null || dbType?.PropertyType != typeof(short) || rowBinding == null || valueOffset == null || readBytes?.ReturnType != typeof(byte[]))
					return;

				var readerParameter  = Expression.Parameter(typeof(DbDataReader), "r");
				var ordinalParameter = Expression.Parameter(typeof(int), "i");
				var binding          = Expression.Variable(bindingType!, "binding");

				Read = Expression.Lambda<Func<DbDataReader, int, byte[]?>>(
					Expression.Block(
						[binding],
						Expression.Assign(binding, Expression.Call(Expression.Convert(readerParameter, readerType), getColumnBinding, ordinalParameter)),
						Expression.Condition(
							Expression.Equal(Expression.Property(binding, dbType), Expression.Constant(DBTYPE_NUMERIC)),
							Expression.Call(Expression.Property(binding, rowBinding), readBytes, Expression.Property(binding, valueOffset), Expression.Constant(DbNumericSize)),
							Expression.Constant(null, typeof(byte[])))),
					readerParameter,
					ordinalParameter)
					.CompileExpression();
			}

			public Type                             ReaderType { get; }
			public Func<DbDataReader, int, byte[]?>? Read      { get; }
		}
	}
}
