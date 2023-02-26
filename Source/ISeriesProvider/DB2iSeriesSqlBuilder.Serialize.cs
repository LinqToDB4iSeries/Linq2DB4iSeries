using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.SqlQuery;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal partial class DB2iSeriesSqlBuilder
	{
		public static string UnnamedParameterMarker { get; } = "?";
		public static string NamedQueryParameterMarkerPrefix { get; } = "@";
		public static string NamedStoredProcedureParameterMarkerPrefix { get; } = ":";

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

		private static readonly string[] sqlDateTimeFormats = Enumerable.Range(0, 8)
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

#if NET6_0_OR_GREATER
		public static void ConvertDateOnlyToSql(StringBuilder stringBuilder, DateOnly value, bool quoted = true)
		{
			if (quoted) stringBuilder.Append('\'');
			stringBuilder.AppendFormat("{0:yyyy-MM-dd}", value);
			if (quoted) stringBuilder.Append('\'');
		}

		public static DateOnly ParseDateOnly(string value)
		{
			if (DateOnly.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out var res))
				return res;

			return DateOnly.ParseExact(
				value,
				"yyyy-MM-dd",
				CultureInfo.InvariantCulture,
				DateTimeStyles.None);
		}
#endif

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
				return sqlDateTimeFormats.Last();
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

		public static void ConvertBinaryToSql(StringBuilder stringBuilder, byte[] value)
		{
			stringBuilder
				.Append("BX'")
				.AppendByteArrayAsHexViaLookup32(value)
				.Append('\'');
		}

		public static void ConvertGuidToSql(StringBuilder stringBuilder, Guid value)
		{
			stringBuilder
			  .Append("CAST(");

			ConvertBinaryToSql(stringBuilder, value.ToByteArray());
			
			stringBuilder
			  .Append(" AS ")
			  .Append(Constants.DbTypes.Char16ForBitData)
			  .Append(')');
		}

		public static string GetDbType(string name, int? length, int? precision, int? scale)
		{
			if (name is null)
				return null;

			if (name.Contains('('))
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
	}
}
