using System;
using System.Collections.Generic;
using System.Globalization;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class SqlNumberParser
	{
		public static byte ParseByte(string str)
		{
			return NumberParser<byte>.Parse(str, byte.MinValue, byte.MaxValue, (s, style, provider) =>
			{
				var success = byte.TryParse(s, style, provider, out var value);
				return (value, success);
			}, dbValue => (byte)dbValue);
		}

		public static short ParseInt16(string str)
		{
			return NumberParser<short>.Parse(str, short.MinValue, short.MaxValue, (s, style, provider) =>
			{
				var success = short.TryParse(s, style, provider, out var value);
				return (value, success);
			}, dbValue => (short)dbValue);
		}

		public static ushort ParseUInt16(string str)
		{
			return NumberParser<ushort>.Parse(str, ushort.MinValue, ushort.MaxValue, (s, style, provider) =>
			{
				var success = ushort.TryParse(s, style, provider, out var value);
				return (value, success);
			}, dbValue => (ushort)dbValue);
		}

		public static int ParseInt32(string str)
		{
			return NumberParser<int>.Parse(str, int.MinValue, int.MaxValue, (s, style, provider) =>
			{
				var success = int.TryParse(s, style, provider, out var value);
				return (value, success);
			}, dbValue => (int)dbValue);
		}

		public static uint ParseUInt32(string str)
		{
			return NumberParser<uint>.Parse(str, uint.MinValue, uint.MaxValue, (s, style, provider) =>
			{
				var success = uint.TryParse(s, style, provider, out var value);
				return (value, success);
			}, dbValue => (uint)dbValue);
		}

		public static long ParseInt64(string str)
		{
			return NumberParser<long>.Parse(str, long.MinValue, long.MaxValue, (s, style, provider) =>
			{
				var success = long.TryParse(s, style, provider, out var value);
				return (value, success);
			}, dbValue => (long)dbValue);
		}

		public static ulong ParseUInt64(string str)
		{
			return NumberParser<ulong>.Parse(str, ulong.MinValue, ulong.MaxValue, (s, style, provider) =>
			{
				var success = ulong.TryParse(s, style, provider, out var value);
				return (value, success);
			}, dbValue => (ulong)dbValue);
		}

		private class NumberParser<T>
			where T : struct
		{
			public static T Parse(string str, T minValue, T maxValue, Func<string, NumberStyles, IFormatProvider, (T Value, bool Success)> tryParseFunc, Func<decimal, T> castFromDecimalFunc)
			{
				if (string.IsNullOrWhiteSpace(str))
					throw new ArgumentNullException(nameof(str));
				
				// Fast path: Typical parsing with InvariantCulture
				var tryParseResult = tryParseFunc(str, NumberStyles.Any, CultureInfo.InvariantCulture);
				
				if (tryParseResult.Success)
				{
					return tryParseResult.Value;
				}

				// Safe path with Decimal: Check for numbers with zero decimal (e.g. "35.0")
				if (decimal.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var dbValue))
				{
					var decimalPart = dbValue % 1;

					// If it has a real decimal part (e.g. 35.5), throw a strict exception
					if (decimalPart != 0)
					{
						throw new FormatException(
							$"Global Converter Safety Alert: Cannot convert string '{str}' to type {typeof(T).Name} because it contains a non-zero fractional part ({decimalPart}). Conversion aborted to prevent data loss.");
					}

					var convertedValue = castFromDecimalFunc(dbValue);

					// Check for overflow: Ensure the value is within the bounds of T
					if (Comparer<T>.Default.Compare(convertedValue, minValue) < 0 || Comparer<T>.Default.Compare(convertedValue, maxValue) > 0)
					{
						throw new OverflowException(
							$"Global Converter Safety Alert: The value '{str}' is too large or too small for type {typeof(T).Name} (Min: {minValue}, Max: {maxValue}).");
					}
					
					return convertedValue;
				}

				// Fallback: If all else fails, attempt a direct parse with InvariantCulture, which will throw the default exceptions for invalid formats or overflows
				return (T)Convert.ChangeType(str, typeof(T), CultureInfo.InvariantCulture);
			}

		}
	}
}
