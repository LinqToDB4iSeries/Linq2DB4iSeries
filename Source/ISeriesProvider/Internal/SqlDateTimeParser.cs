using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class SqlDateTimeParser
	{
		private static readonly string[] parseDateTimeFormats = Enumerable.Range(-1, 9)
			.Select(x => x switch
			{
				-1 => "",
				0 => "-HH.mm.ss",
				_ => $"-HH.mm.ss.{new string('f', x)}",
			})
			.Select(x => $"yyyy-MM-dd{x}")
			.ToArray();

		private static readonly string[] parseTimeFormats = new[]
		{
			@"hh\.mm\.ss"
		};

		public static DateTime ParseDateTime(string value)
		{
			if (DateTime.TryParseExact(value, parseDateTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
				return result;
			else if (DateTime.TryParseExact(value, "HH.mm.ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
				return new DateTime(1, 1, 1, result.Hour, result.Minute, result.Second);
			else
				return DateTime.Parse(value);
		}

		public static TimeSpan ParseTimeSpan(string value)
		{
			if (TimeSpan.TryParse(value, out var res))
				return res;
			else
				return TimeSpan.ParseExact(value, parseTimeFormats, CultureInfo.InvariantCulture, TimeSpanStyles.None);
		}
	}
}
