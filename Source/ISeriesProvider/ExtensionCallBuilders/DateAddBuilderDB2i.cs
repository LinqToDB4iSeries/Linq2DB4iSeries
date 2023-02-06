using System;
using LinqToDB.SqlQuery;
using static LinqToDB.Sql;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class DateAddBuilderDB2i : IExtensionCallBuilder
	{
		public void Build(ISqExtensionBuilder builder)
		{
			var part = builder.GetValue<DateParts>("part");
			var date = builder.GetExpression("date");
			var number = builder.GetExpression("number");
			
			var expStr = part switch
			{
				DateParts.Year => "{0} + ({1}) Year",
				DateParts.Quarter => "{0} + (({1}) * 3) Month",
				DateParts.Month => "{0} + ({1}) Month",
				DateParts.DayOfYear or DateParts.WeekDay or DateParts.Day => "{0} + ({1}) Day",
				DateParts.Week => "{0} + (({1}) * 7) Day",
				DateParts.Hour => "{0} + ({1}) Hour",
				DateParts.Minute => "{0} + ({1}) Minute",
				DateParts.Second => "{0} + ({1}) Second",
				DateParts.Millisecond => "{0} + (({1}) * CAST(1000 AS BIGINT)) Microsecond",
				_ => throw new ArgumentOutOfRangeException("part"),
			};

			builder.ResultExpression = new SqlExpression(typeof(DateTime?), expStr, Precedence.Additive, date, number);
		}
	}
}
