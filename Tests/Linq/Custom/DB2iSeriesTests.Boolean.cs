using System;
using System.Linq;

using LinqToDB;
using LinqToDB.Mapping;
using LinqToDB.Tools.Comparers;

using NUnit.Framework;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		// Adapted from Tests/Linq/Linq/BooleanTests.cs (linq2db v6.5.1).
		// Copy the assertions because the upstream helpers cannot accept an in-memory
		// baseline and otherwise fetch the full table again for every comparison.
		sealed class BooleanCoverageRow
		{
			static BooleanCoverageRow[] GetData()
			{
				var id = 1;
				// Numeric types are tested independently. Share sign/null states instead
				// of multiplying three identical matrices (72 rows instead of 10,368).
				return (
					from boolean in new[] { true, false }
					from booleanN in new bool?[] { true, false, null }
					from number in new[] { -1, 0, 1 }
					from numberN in new int?[] { -1, 0, 1, null }
					select new BooleanCoverageRow
					{
						Id       = id++,
						Boolean  = boolean,
						BooleanN = booleanN,
						Int32    = number,
						Int32N   = numberN,
						Decimal  = number / 10m,
						DecimalN = numberN / 10m,
						Double   = number / 10.0,
						DoubleN  = numberN / 10.0,
					})
					.ToArray();
			}

			[PrimaryKey] public int Id { get; set; }
			[Column] public bool Boolean { get; set; }
			[Column] public bool? BooleanN { get; set; }
			[Column] public int Int32 { get; set; }
			[Column] public int? Int32N { get; set; }
			[Column] public decimal Decimal { get; set; }
			[Column] public decimal? DecimalN { get; set; }
			[Column] public double Double { get; set; }
			[Column] public double? DoubleN { get; set; }

			public static readonly BooleanCoverageRow[] Data = GetData();
		}

		[Test]
		public void BooleanExpressionsAndCoalesce([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			var data = BooleanCoverageRow.Data;
			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable(data);

			foreach (var inline in new[] { false, true })
			{
				db.InlineParameters = inline;
				CheckBooleanExpressions();
				CheckCoalesce();
			}

			void Check<T>(Func<IQueryable<BooleanCoverageRow>, IQueryable<T>> build)
			{
				var expected = build(data.AsQueryable()).ToArray();
				var actual = build(table).ToArray();
				// Multiset comparison ignores SQL ordering while checking duplicate multiplicity.
				Assert.That(actual, Is.EquivalentTo(expected).Using(ComparerBuilder.GetEqualityComparer<T>()));
			}

			void CheckCoalesce()
			{
				var True = true;
				var False = false;

				// Each BooleanN state spans 12 consecutive IDs per Boolean value,
				// covering odd and even IDs for the expression-based fallback.
				Check(rows => rows.Where(r => r.BooleanN ?? True));
				Check(rows => rows.Where(r => r.BooleanN ?? False));
				Check(rows => rows.Where(r => r.BooleanN ?? (r.Id % 2 == 1)));
			}

			void CheckBooleanExpressions()
			{
				var True = true;
				var False = false;
				bool? TrueN = true;
				bool? FalseN = false;
				bool? Null = null;

				Check(rows => rows.Where(r => r.Boolean == True));
				Check(rows => rows.Where(r => r.Boolean == False));
				Check(rows => rows.Where(r => r.Boolean == TrueN));
				Check(rows => rows.Where(r => r.Boolean == FalseN));
				Check(rows => rows.Where(r => r.Boolean == Null));

				Check(rows => rows.Where(r => r.BooleanN == True));
				Check(rows => rows.Where(r => r.BooleanN == False));
				Check(rows => rows.Where(r => r.BooleanN == TrueN));
				Check(rows => rows.Where(r => r.BooleanN == FalseN));
				Check(rows => rows.Where(r => r.BooleanN == Null));

				Check(rows => rows.Where(r => r.Boolean != True));
				Check(rows => rows.Where(r => r.Boolean != False));
				Check(rows => rows.Where(r => r.Boolean != TrueN));
				Check(rows => rows.Where(r => r.Boolean != FalseN));
				Check(rows => rows.Where(r => r.Boolean != Null));

				Check(rows => rows.Where(r => r.BooleanN != True));
				Check(rows => rows.Where(r => r.BooleanN != False));
				Check(rows => rows.Where(r => r.BooleanN != TrueN));
				Check(rows => rows.Where(r => r.BooleanN != FalseN));
				Check(rows => rows.Where(r => r.BooleanN != Null));

				Check(rows => rows.GroupBy(r => r.Id)
					.Select(g => new
					{
						g.Key,

						Count = g.Count(r => r.Boolean),
						Count_Explicit = g.Count(r => r.Boolean == true),
						CountN = g.Count(r => r.BooleanN == true),

						Count_False = g.Count(r => r.Boolean == false),
						CountN_False = g.Count(r => r.BooleanN == false),

						Count_NotTrue = g.Count(r => r.Boolean != true),
						CountN_NotTrue = g.Count(r => r.BooleanN != true),

						Count_NotFalse = g.Count(r => r.Boolean != false),
						CountN_NotFalse = g.Count(r => r.BooleanN != false),

						CountInt32 = g.Count(r => r.Int32 == 0),
						Count32N = g.Count(r => r.Int32N == 0),
						CountDecimal = g.Count(r => r.Decimal == 0),
						CountDecimalN = g.Count(r => r.DecimalN == 0),
						CountDouble = g.Count(r => r.Double == 0),
						CountDoubleN = g.Count(r => r.DoubleN == 0),

						CountInt32_NotEqual = g.Count(r => r.Int32 != 0),
						Count32N_NotEqual = g.Count(r => r.Int32N != 0),
						CountDecimal_NotEqual = g.Count(r => r.Decimal != 0),
						CountDecimalN_NotEqual = g.Count(r => r.DecimalN != 0),
						CountDouble_NotEqual = g.Count(r => r.Double != 0),
						CountDoubleN_NotEqual = g.Count(r => r.DoubleN != 0),

						CountInt32_Greater = g.Count(r => r.Int32 > 0),
						Count32N_Greater = g.Count(r => r.Int32N > 0),
						CountDecimal_Greater = g.Count(r => r.Decimal > 0),
						CountDecimalN_Greater = g.Count(r => r.DecimalN > 0),
						CountDouble_Greater = g.Count(r => r.Double > 0),
						CountDoubleN_Greater = g.Count(r => r.DoubleN > 0),

						CountInt32_Less = g.Count(r => r.Int32 < 0),
						Count32N_Less = g.Count(r => r.Int32N < 0),
						CountDecimal_Less = g.Count(r => r.Decimal < 0),
						CountDecimalN_Less = g.Count(r => r.DecimalN < 0),
						CountDouble_Less = g.Count(r => r.Double < 0),
						CountDoubleN_Less = g.Count(r => r.DoubleN < 0),

						CountInt32_GreaterEqual = g.Count(r => r.Int32 >= 0),
						Count32N_GreaterEqual = g.Count(r => r.Int32N >= 0),
						CountDecimal_GreaterEqual = g.Count(r => r.Decimal >= 0),
						CountDecimalN_GreaterEqual = g.Count(r => r.DecimalN >= 0),
						CountDouble_GreaterEqual = g.Count(r => r.Double >= 0),
						CountDoubleN_GreaterEqual = g.Count(r => r.DoubleN >= 0),

						CountInt32_LessEqual = g.Count(r => r.Int32 <= 0),
						Count32N_LessEqual = g.Count(r => r.Int32N <= 0),
						CountDecimal_LessEqual = g.Count(r => r.Decimal <= 0),
						CountDecimalN_LessEqual = g.Count(r => r.DecimalN <= 0),
						CountDouble_LessEqual = g.Count(r => r.Double <= 0),
						CountDoubleN_LessEqual = g.Count(r => r.DoubleN <= 0),
					}));

				var query = (IQueryable<BooleanCoverageRow> rows) => rows.Select(r => new
				{
					r.Id,

					Condition1 = r.Int32 == 0,
					Condition2 = r.Int32N == 0,
					Condition3 = r.Decimal == 0,
					Condition4 = r.DecimalN == 0,
					Condition5 = r.Double == 0,
					Condition6 = r.DoubleN == 0,
					Condition11 = (r.Int32N ?? r.Int32) == 0,
					Condition12 = (r.DecimalN ?? r.Decimal) == 0,
					Condition13 = (r.DoubleN ?? r.Double) == 0,
					Condition21 = (r.Boolean ? r.Int32N : r.Int32) == 0,
					Condition22 = (r.BooleanN == false ? r.Int32N : r.Int32) == 0,
					Condition23 = (r.Boolean ? r.DecimalN : r.Decimal) == 0,
					Condition24 = (r.BooleanN == false ? r.DecimalN : r.Decimal) == 0,
					Condition25 = (r.Boolean ? r.DoubleN : r.Double) == 0,
					Condition26 = (r.BooleanN == false ? r.DoubleN : r.Double) == 0,

					Condition101 = r.Int32 > 0,
					Condition102 = r.Int32N > 0,
					Condition103 = r.Decimal > 0,
					Condition104 = r.DecimalN > 0,
					Condition105 = r.Double > 0,
					Condition106 = r.DoubleN > 0,
					Condition111 = (r.Int32N ?? r.Int32) > 0,
					Condition112 = (r.DecimalN ?? r.Decimal) > 0,
					Condition113 = (r.DoubleN ?? r.Double) > 0,
					Condition121 = (r.Boolean ? r.Int32N : r.Int32) > 0,
					Condition122 = (r.BooleanN == false ? r.Int32N : r.Int32) > 0,
					Condition123 = (r.Boolean ? r.DecimalN : r.Decimal) > 0,
					Condition124 = (r.BooleanN == false ? r.DecimalN : r.Decimal) > 0,
					Condition125 = (r.Boolean ? r.DoubleN : r.Double) > 0,
					Condition126 = (r.BooleanN == false ? r.DoubleN : r.Double) > 0,

					Condition201 = r.Int32 >= 0,
					Condition202 = r.Int32N >= 0,
					Condition203 = r.Decimal >= 0,
					Condition204 = r.DecimalN >= 0,
					Condition205 = r.Double >= 0,
					Condition206 = r.DoubleN >= 0,
					Condition211 = (r.Int32N ?? r.Int32) >= 0,
					Condition212 = (r.DecimalN ?? r.Decimal) >= 0,
					Condition213 = (r.DoubleN ?? r.Double) >= 0,
					Condition221 = (r.Boolean ? r.Int32N : r.Int32) >= 0,
					Condition222 = (r.BooleanN == false ? r.Int32N : r.Int32) >= 0,
					Condition223 = (r.Boolean ? r.DecimalN : r.Decimal) >= 0,
					Condition224 = (r.BooleanN == false ? r.DecimalN : r.Decimal) >= 0,
					Condition225 = (r.Boolean ? r.DoubleN : r.Double) >= 0,
					Condition226 = (r.BooleanN == false ? r.DoubleN : r.Double) >= 0,

					Condition301 = r.Int32 < 0,
					Condition302 = r.Int32N < 0,
					Condition303 = r.Decimal < 0,
					Condition304 = r.DecimalN < 0,
					Condition305 = r.Double < 0,
					Condition306 = r.DoubleN < 0,
					Condition311 = (r.Int32N ?? r.Int32) < 0,
					Condition312 = (r.DecimalN ?? r.Decimal) < 0,
					Condition313 = (r.DoubleN ?? r.Double) < 0,
					Condition321 = (r.Boolean ? r.Int32N : r.Int32) < 0,
					Condition322 = (r.BooleanN == false ? r.Int32N : r.Int32) < 0,
					Condition323 = (r.Boolean ? r.DecimalN : r.Decimal) < 0,
					Condition324 = (r.BooleanN == false ? r.DecimalN : r.Decimal) < 0,
					Condition325 = (r.Boolean ? r.DoubleN : r.Double) < 0,
					Condition326 = (r.BooleanN == false ? r.DoubleN : r.Double) < 0,

					Condition401 = r.Int32 <= 0,
					Condition402 = r.Int32N <= 0,
					Condition403 = r.Decimal <= 0,
					Condition404 = r.DecimalN <= 0,
					Condition405 = r.Double <= 0,
					Condition406 = r.DoubleN <= 0,
					Condition411 = (r.Int32N ?? r.Int32) <= 0,
					Condition412 = (r.DecimalN ?? r.Decimal) <= 0,
					Condition413 = (r.DoubleN ?? r.Double) <= 0,
					Condition421 = (r.Boolean ? r.Int32N : r.Int32) <= 0,
					Condition422 = (r.BooleanN == false ? r.Int32N : r.Int32) <= 0,
					Condition423 = (r.Boolean ? r.DecimalN : r.Decimal) <= 0,
					Condition424 = (r.BooleanN == false ? r.DecimalN : r.Decimal) <= 0,
					Condition425 = (r.Boolean ? r.DoubleN : r.Double) <= 0,
					Condition426 = (r.BooleanN == false ? r.DoubleN : r.Double) <= 0,
				});

				Check(rows => query(rows).Concat(query(rows)));
			}
		}
	}
}
