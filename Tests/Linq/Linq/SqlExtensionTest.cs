// TODO : Should get these working

//using System;
//using System.Globalization;
//using System.Linq;
//using LinqToDB;
//using LinqToDB.SqlQuery;
//using NUnit.Framework;

//namespace Tests.Linq
//{
//	//using PN = ProviderName;

//	public static class TestedExtensions
//	{
//		class DatePartBuilderDB2 : Sql.IExtensionCallBuilder
//		{
//			public void Build(Sql.ISqExtensionBuilder builder)
//			{
//				string partStr;
//				var part = builder.GetValue<Sql.DateParts>("part");
//				switch (part)
//				{
//					case Sql.DateParts.Year: partStr = "To_Number(To_Char({date}, 'YYYY'))"; break;
//					case Sql.DateParts.Quarter: partStr = "To_Number(To_Char({date}, 'Q'))"; break;
//					case Sql.DateParts.Month: partStr = "To_Number(To_Char({date}, 'MM'))"; break;
//					case Sql.DateParts.DayOfYear: partStr = "To_Number(To_Char({date}, 'DDD'))"; break;
//					case Sql.DateParts.Day: partStr = "To_Number(To_Char({date}, 'DD'))"; break;
//					case Sql.DateParts.Week: partStr = "To_Number(To_Char({date}, 'WW'))"; break;
//					case Sql.DateParts.WeekDay: partStr = "DayOfWeek({date})"; break;
//					case Sql.DateParts.Hour: partStr = "To_Number(To_Char({date}, 'HH24'))"; break;
//					case Sql.DateParts.Minute: partStr = "To_Number(To_Char({date}, 'MI'))"; break;
//					case Sql.DateParts.Second: partStr = "To_Number(To_Char({date}, 'SS'))"; break;
//					case Sql.DateParts.Millisecond: partStr = "To_Number(To_Char({date}, 'FF')) / 1000"; break;
//					default:
//						throw new ArgumentOutOfRangeException();
//				}

//				builder.Expression = partStr;
//			}
//		}


//		[Sql.Extension("", ServerSideOnly = false, BuilderType = typeof(DatePartBuilderDB2))] // TODO: Not checked
//		public static int? DatePart(this Sql.ISqlExtension ext, Sql.DateParts part, [ExprParameter] DateTime? date)
//		{
//			if (date == null)
//				return null;

//			switch (part)
//			{
//				case Sql.DateParts.Year: return date.Value.Year;
//				case Sql.DateParts.Quarter: return (date.Value.Month - 1) / 3 + 1;
//				case Sql.DateParts.Month: return date.Value.Month;
//				case Sql.DateParts.DayOfYear: return date.Value.DayOfYear;
//				case Sql.DateParts.Day: return date.Value.Day;
//				case Sql.DateParts.Week: return CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(date.Value, CalendarWeekRule.FirstDay, DayOfWeek.Sunday);
//				case Sql.DateParts.WeekDay: return ((int)date.Value.DayOfWeek + 1 + Sql.DateFirst + 6) % 7 + 1;
//				case Sql.DateParts.Hour: return date.Value.Hour;
//				case Sql.DateParts.Minute: return date.Value.Minute;
//				case Sql.DateParts.Second: return date.Value.Second;
//				case Sql.DateParts.Millisecond: return date.Value.Millisecond;
//			}

//			throw new InvalidOperationException();
//		}
//	}

//	public class SqlExtensionTests : TestBase
//	{
//		#region DatePart

//		[Test, DataContextSource]
//		public void DatePartYear(string context)
//		{
//			using (var db = GetDataContext(context))
//			{
//				var expected = from t in Types select Sql.Ext.DatePart(Sql.DateParts.Year, t.DateTimeValue);
//				var actual = from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Year, t.DateTimeValue));
//				var spm = actual.ToList();
//				AreEqual(expected, actual);
//			}
//		}

//		[Test, DataContextSource]
//		public void DatePartQuarter(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.Quarter, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Quarter, t.DateTimeValue)));
//		}

//		[Test, DataContextSource]
//		public void DatePartMonth(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.Month, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Month, t.DateTimeValue)));
//		}

//		[Test, DataContextSource]
//		public void DatePartDayOfYear(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.DayOfYear, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.DayOfYear, t.DateTimeValue)));
//		}

//		[Test, DataContextSource]
//		public void DatePartDay(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.Day, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Day, t.DateTimeValue)));
//		}

//		[Test, DataContextSource]
//		public void DatePartWeek(string context)
//		{
//			using (var db = GetDataContext(context))
//				(from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Week, t.DateTimeValue))).ToList();
//		}

//		[Test, DataContextSource]
//		public void DatePartWeekDay(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.WeekDay, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.WeekDay, t.DateTimeValue)));
//		}

//		[Test, DataContextSource]
//		public void DatePartHour(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.Hour, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Hour, t.DateTimeValue)));
//		}

//		[Test, DataContextSource]
//		public void DatePartMinute(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.Minute, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Minute, t.DateTimeValue)));
//		}

//		[Test, DataContextSource]
//		public void DatePartSecond(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.Second, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Second, t.DateTimeValue)));
//		}

//		[Test, DataContextSource(ProviderName.Informix, ProviderName.MySql, ProviderName.Access, ProviderName.SapHana, TestProvName.MariaDB)]
//		public void DatePartMillisecond(string context)
//		{
//			using (var db = GetDataContext(context))
//				AreEqual(
//					from t in Types select Sql.Ext.DatePart(Sql.DateParts.Millisecond, t.DateTimeValue),
//					from t in db.Types select Sql.AsSql(Sql.Ext.DatePart(Sql.DateParts.Millisecond, t.DateTimeValue)));
//		}

//		#endregion
//	}
//}