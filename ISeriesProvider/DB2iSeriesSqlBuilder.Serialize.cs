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
			stringBuilder.Append("CHR(").Append(value).Append(")");
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

		public static void ConvertDateTimeToSql(StringBuilder stringBuilder, DataType datatype, DateTime value, bool quoted = true)
		{
			var format = datatype switch
			{
				DataType.Date => "{0:yyyy-MM-dd}",
				DataType.Time => "{0:HH:mm:ss}",

				_ => value.Millisecond == 0 ?
						"{0:yyyy-MM-dd HH:mm:ss}" :
						"{0:yyyy-MM-dd HH:mm:ss.fffffff}"
			};

			if (quoted) stringBuilder.Append("'");
			stringBuilder.AppendFormat(format, value);
			if (quoted) stringBuilder.Append("'");
		}

		public static string ConvertDateTimeToSql(DataType datatype, DateTime value, bool quoted = true)
		{
			var sb = new StringBuilder();
			ConvertDateTimeToSql(sb, datatype, value, quoted);
			return sb.ToString();
		}

		public static void ConvertTimeToSql(StringBuilder stringBuilder, TimeSpan time, bool quoted = true)
		{
			if (quoted) stringBuilder.Append("'");
			stringBuilder.Append($"{time:hh\\:mm\\:ss}");
			if (quoted) stringBuilder.Append("'");
		}

		public static string ConvertTimeToSql(TimeSpan time, bool quoted = true)
		{
			var sb = new StringBuilder();
			ConvertTimeToSql(sb, time, quoted);
			return sb.ToString();
		}

		public static void ConvertBinaryToSql(StringBuilder stringBuilder, byte[] value)
		{
			stringBuilder.Append("BX'");

			foreach (var b in value)
				stringBuilder.Append(b.ToString("X2"));

			stringBuilder.Append("'");
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
			  .Append(")");
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

			return stringBuilder.ToString();
		}

		public static DateTime ParseDateTime(string value)
		{
			if (DateTime.TryParse(value, out var res))
				return res;

			return DateTime.ParseExact(
				value,
				new[]
				{
					"yyyy-MM-dd",
					"yyyy-MM-dd-HH.mm.ss",
					"yyyy-MM-dd-HH.mm.ss.f",
					"yyyy-MM-dd-HH.mm.ss.ff",
					"yyyy-MM-dd-HH.mm.ss.fff",
					"yyyy-MM-dd-HH.mm.ss.ffff",
					"yyyy-MM-dd-HH.mm.ss.fffff",
					"yyyy-MM-dd-HH.mm.ss.ffffff",
					"yyyy-MM-dd-HH.mm.ss.fffffff",
					"yyyy-MM-dd-HH.mm.ss.ffffffff",
					"yyyy-MM-dd-HH.mm.ss.fffffffff",
					"yyyy-MM-dd-HH.mm.ss.ffffffffff",
					"yyyy-MM-dd-HH.mm.ss.fffffffffff",
					"yyyy-MM-dd-HH.mm.ss.ffffffffffff",
				},
				CultureInfo.InvariantCulture,
				DateTimeStyles.None);
		}
	}
}
