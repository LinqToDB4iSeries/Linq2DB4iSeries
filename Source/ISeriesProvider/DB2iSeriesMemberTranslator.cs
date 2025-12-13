using LinqToDB.Internal.DataProvider.DB2.Translation;
using LinqToDB.Internal.DataProvider.Translation;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Linq.Translation;
using System;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class DB2iSeriesMemberTranslator : DB2MemberTranslator
	{
		protected override IMemberTranslator CreateDateMemberTranslator()
		{
			return new Db2iDateFunctionsTranslator();
		}

		protected class Db2iDateFunctionsTranslator : DateFunctionsTranslator
		{
			protected override ISqlExpression TranslateDateTimeDatePart(ITranslationContext translationContext, TranslationFlags translationFlag, ISqlExpression dateTimeExpression, Sql.DateParts datepart)
			{
				var factory = translationContext.ExpressionFactory;
				var intDataType = factory.GetDbDataType(typeof(int));

				var partStr = datepart switch
				{
					Sql.DateParts.Year => "YEAR({0})",
					Sql.DateParts.Quarter => "QUARTER({0})",
					Sql.DateParts.Month => "MONTH({0})",
					Sql.DateParts.DayOfYear => "DAYOFYEAR({0})",
					Sql.DateParts.Day => "DAY({0})",
					Sql.DateParts.Week => "WEEK({0})",
					Sql.DateParts.WeekDay => "DAYOFWEEK({0})",
					Sql.DateParts.Hour => "HOUR({0})",
					Sql.DateParts.Minute => "MINUTE({0})",
					Sql.DateParts.Second => "SECOND({0})",
					Sql.DateParts.Millisecond => "MICROSECOND({0}) / 1000",
					_ => throw new NotSupportedException($"Sql date part {datepart} is not supported."),
				};

				return factory.Expression(intDataType, partStr, dateTimeExpression);
			}
		}

		protected override IMemberTranslator CreateGuidMemberTranslator()
		{
			return new Db2iSeriesGuidMemberTranslator();
		}

		protected class Db2iSeriesGuidMemberTranslator : GuidMemberTranslator
		{
			protected override ISqlExpression? TranslateGuildToString(ITranslationContext translationContext, MethodCallExpression methodCall, ISqlExpression guidExpr, TranslationFlags translationFlags)
			{
				if (translationContext.MappingSchema.IsGuidMappedAsString() == false)
					return base.TranslateGuildToString(translationContext, methodCall, guidExpr, translationFlags);

				//"LOWER(CAST({0} AS char(36)))"

				var factory = translationContext.ExpressionFactory;
				var stringDataType = factory.GetDbDataType(typeof(string)).WithDataType(DataType.Char).WithLength(36);

				var cast = factory.Cast(guidExpr, stringDataType);
				var lower = factory.ToLower(cast);

				return lower;
			}
		}
	}
}
