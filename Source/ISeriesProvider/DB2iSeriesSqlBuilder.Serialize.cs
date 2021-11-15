using LinqToDB.Common;
using LinqToDB.SqlQuery;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public partial class DB2iSeriesSqlBuilder
	{
		private static void AppendConversion(StringBuilder stringBuilder, int value)
		{
			stringBuilder.Append("CHR(").Append(value).Append(')');
		}

		public static void ConvertStringToSql(StringBuilder stringBuilder, string value)
		{
			DataTools.ConvertStringToSql(stringBuilder, "||", "", AppendConversion, value, null);
		}

		public static string ConvertStringToSql(string value)
		{
			var sb = new StringBuilder();
			ConvertStringToSql(sb, value);
			return sb.ToString();
		}

		public static void ConvertCharToSql(StringBuilder stringBuilder, char value)
		{
			DataTools.ConvertCharToSql(stringBuilder, "'", AppendConversion, value);
		}

		public static string ConvertCharToSql(char value)
		{
			var sb = new StringBuilder();
			ConvertCharToSql(sb, value);
			return sb.ToString();
		}

		private static readonly string[] sqlDateTimeFormats = Enumerable.Range(0, 12)
			.Select(x => x switch
			{
				0 => " HH:mm:ss",
				_ => $" HH:mm:ss.{new string('f', x)}",
			})
			.Select(x => $"{{0:yyyy-MM-dd{x}}}")
			.ToArray();

		public static void ConvertDateTimeToSql(StringBuilder stringBuilder, DataType datatype, DateTime value, bool quoted = true, int? precision = null)
		{
			var format = datatype switch
			{
				DataType.Date => "{0:yyyy-MM-dd}",
				DataType.Time => "{0:HH:mm:ss}",
				_ => GetSqlDateTimeFormat(value.Millisecond, precision)
			};

			if (quoted) stringBuilder.Append('\'');
			stringBuilder.AppendFormat(format, value);
			if (quoted) stringBuilder.Append('\'');
		}

		public static string ConvertDateTimeToSql(DataType datatype, DateTime value, bool quoted = true, int? precision = null)
		{
			var sb = new StringBuilder();
			ConvertDateTimeToSql(sb, datatype, value, quoted, precision);
			return sb.ToString();
		}

		/// <summary>
		/// The Ole Db provider requires DateTime strings to match the DB type's precision.
		/// </summary>
		private static string GetSqlDateTimeFormat(int value, int? precision)
		{
			if (value == 0 || precision == 0)
				return sqlDateTimeFormats[0];
			else if (precision == null)
				return sqlDateTimeFormats[6];
			else if (precision.Value <= sqlDateTimeFormats.Length)
				return sqlDateTimeFormats[precision.Value];
			else
				return $"{sqlDateTimeFormats[0]}.{new string('f', precision.Value)}";
		}

		public static void ConvertTimeToSql(StringBuilder stringBuilder, TimeSpan time, bool quoted = true)
		{
			if (quoted) stringBuilder.Append('\'');
			stringBuilder.Append($"{time:hh\\:mm\\:ss}");
			if (quoted) stringBuilder.Append('\'');
		}

		public static string ConvertTimeToSql(TimeSpan time, bool quoted = true)
		{
			var sb = new StringBuilder();
			ConvertTimeToSql(sb, time, quoted);
			return sb.ToString();
		}

		public static void ConvertDoubleToSql(StringBuilder sb, double value)
		{
			sb.Append("CAST(").Append(value.ToString("R")).Append(" AS DOUBLE)");
		}

		public static void ConvertInt64ToSql(StringBuilder sb, long value)
		{
			sb.Append("CAST(").Append(value).Append(" AS BIGINT)");
		}

		public static void ConvertBinaryToSql(StringBuilder stringBuilder, byte[] value)
		{
			stringBuilder.Append("BX'");

			foreach (var b in value)
				stringBuilder.Append(b.ToString("X2"));

			stringBuilder.Append('\'');
		}

		public static string ConvertBinaryToSql(byte[] value)
		{
			var sb = new StringBuilder();
			ConvertBinaryToSql(sb, value);
			return sb.ToString();
		}

		public static void ConvertGuidToSql(StringBuilder stringBuilder, Guid value)
		{
			var s = value.ToString("N");
			stringBuilder
			  .Append("CAST(x'")
			  .Append(s.Substring(6, 2))
			  .Append(s.Substring(4, 2))
			  .Append(s.Substring(2, 2))
			  .Append(s.Substring(0, 2))
			  .Append(s.Substring(10, 2))
			  .Append(s.Substring(8, 2))
			  .Append(s.Substring(14, 2))
			  .Append(s.Substring(12, 2))
			  .Append(s.Substring(16, 16))
			  .Append("' AS ")
			  .Append(Constants.DbTypes.Char16ForBitData)
			  .Append(')');
		}

		public static string ConvertGuidToSql(Guid value)
		{
			var sb = new StringBuilder();
			ConvertGuidToSql(sb, value);
			return sb.ToString();
		}

		public static string GetDbType(string name, int? length, int? precision, int? scale)
		{
			if (name is null)
				return null;

			if (name.Contains("("))
				return name;

			var ccsid = string.Empty;
			var ccsidIndex = name.IndexOf(" CCSID ", StringComparison.OrdinalIgnoreCase);
			if (ccsidIndex > 0)
			{
				ccsid = name.Substring(ccsidIndex);
				name = name.Substring(0, ccsidIndex);
			}

			var stringBuilder = new StringBuilder();

			stringBuilder.Append(name);

			if (length > 0)
				stringBuilder.Append('(').Append(length).Append(')');

			else if (precision >= 0)
			{
				stringBuilder.Append('(').Append(precision);
				if (scale >= 0)
					stringBuilder.Append(", ").Append(scale);
				stringBuilder.Append(')');
			}

			stringBuilder.Append(ccsid);

			return stringBuilder.ToString();
		}

		public static string TrimString(string value)
		{
			if (value == null)
			{
				return null;
			}

			return value.TrimEnd(' ');
		}

		private static readonly string[] parseDateTimeFormats = Enumerable.Range(-1, 14)
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
