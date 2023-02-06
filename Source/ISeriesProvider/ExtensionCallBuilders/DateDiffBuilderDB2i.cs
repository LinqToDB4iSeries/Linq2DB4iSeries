using System;
using LinqToDB.SqlQuery;
using static LinqToDB.Sql;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class DateDiffBuilderDB2i : IExtensionCallBuilder
	{
		public void Build(ISqExtensionBuilder builder)
		{
			var part = builder.GetValue<DateParts>(0);
			var startDate = builder.GetExpression(1);
			var endDate = builder.GetExpression(2);

			var secondsExpr = builder.Mul<int>(builder.Sub<int>(
					new SqlFunction(typeof(int), "Days", endDate),
					new SqlFunction(typeof(int), "Days", startDate)),
				new SqlValue(86400));

			var midnight = builder.Sub<int>(
				new SqlFunction(typeof(int), "MIDNIGHT_SECONDS", endDate),
				new SqlFunction(typeof(int), "MIDNIGHT_SECONDS", startDate));

			var resultExpr = builder.Add<int>(secondsExpr, midnight);

			switch (part)
			{
				case DateParts.Day: resultExpr = builder.Div(resultExpr, 86400); break;
				case DateParts.Hour: resultExpr = builder.Div(resultExpr, 3600); break;
				case DateParts.Minute: resultExpr = builder.Div(resultExpr, 60); break;
				case DateParts.Second: break;
				case DateParts.Millisecond:
					resultExpr = builder.Add<int>(
						builder.Mul(resultExpr, 1000),
						builder.Div(
							builder.Sub<int>(
								new SqlFunction(typeof(int), "MICROSECOND", endDate),
								new SqlFunction(typeof(int), "MICROSECOND", startDate)),
							1000));
					break;
				default:
					throw new ArgumentOutOfRangeException("part");
			}

			builder.ResultExpression = resultExpr;
		}
	}
}
