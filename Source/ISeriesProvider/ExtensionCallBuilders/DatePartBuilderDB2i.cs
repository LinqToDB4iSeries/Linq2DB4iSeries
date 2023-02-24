using System;
using static LinqToDB.Sql;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class DatePartBuilderDB2i : IExtensionCallBuilder
	{
		public void Build(ISqExtensionBuilder builder)
		{
			var part = builder.GetValue<DateParts>("part");
			
			var partStr = part switch
			{
				DateParts.Year => "YEAR({date})",
				DateParts.Quarter => "QUARTER({date})",
				DateParts.Month => "MONTH({date})",
				DateParts.DayOfYear => "DAYOFYEAR({date})",
				DateParts.Day => "DAY({date})",
				DateParts.Week => "WEEK({date})",
				DateParts.WeekDay => "DAYOFWEEK({date})",
				DateParts.Hour => "HOUR({date})",
				DateParts.Minute => "MINUTE({date})",
				DateParts.Second => "SECOND({date})",
				DateParts.Millisecond => "MICROSECOND({date}) / 1000",
				_ => throw new ArgumentOutOfRangeException("part"),
			};

			builder.Expression = partStr;
		}
	}
}
