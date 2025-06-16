using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class PrecisionHelper
	{
		public static int GetScale(decimal value)
		{
			if (value == 0)
				return 0;

#if SUPPORTS_SPAN
			Span<int> bits = stackalloc int[4];
			decimal.GetBits(value, bits);
#else
			var bits = decimal.GetBits(value);
#endif

			return (bits[3] >> 16) & 0x7F;
		}

		public static int GetPrecision(decimal value)
		{
			if (value == 0)
				return 0;

#if SUPPORTS_SPAN
			Span<int> bits = stackalloc int[4];
			decimal.GetBits(value, bits);
#else
			var bits = decimal.GetBits(value);
#endif

			//We will use false for the sign (false =  positive), because we don't care about it.
			//We will use 0 for the last argument instead of bits[3] to eliminate the fraction point.
			var d = new decimal(bits[0], bits[1], bits[2], false, 0);
			return (int)Math.Floor(Math.Log10((double)d)) + 1;
		}

		public static int GetPrecision(DateTime dateTime)
			=> GetTimeSpanPrecision(dateTime.Ticks);

		public static int GetPrecision(DateTimeOffset dateTimeOffset)
			=> GetTimeSpanPrecision(dateTimeOffset.Ticks);

		public static int GetPrecision(TimeSpan timeSpan)
			=> GetTimeSpanPrecision(timeSpan.Ticks);

#if NET6_0_OR_GREATER
		public static int GetPrecision(DateOnly dateOnly)
			=> GetTimeSpanPrecision(dateOnly.ToDateTime(TimeOnly.MinValue).Ticks);
#endif

		private static int GetTimeSpanPrecision(long ticks)
		{
			// Get fractional ticks within the second
			long fractionalTicks = ticks % TimeSpan.TicksPerSecond;

			if (fractionalTicks == 0)
				return 0;

			// Multiply ticks (100 ns) by 100 to convert to nanoseconds
			long ns = fractionalTicks * 100;

			// Strip trailing zeroes to find significant digits
			int precision = 9; // nanoseconds = 9 digits max in .NET
			while (ns % 10 == 0)
			{
				ns /= 10;
				precision--;
			}

			return precision;
		}
	}
}
