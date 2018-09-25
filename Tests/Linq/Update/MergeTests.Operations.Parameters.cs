using System;
using System.Data.SqlClient;
using System.Linq;

using LinqToDB;
using LinqToDB.DataProvider.DB2iSeries;
using NUnit.Framework;

// ReSharper disable once CheckNamespace
namespace Tests.xUpdate
{
    using Model;

    public partial class MergeTests
    {
        // ASE: just fails
        [Test, MergeDataContextSource(ProviderName.Oracle, ProviderName.OracleManaged, ProviderName.OracleNative,
            ProviderName.Sybase, ProviderName.Informix, ProviderName.SapHana, ProviderName.Firebird)]
        public void TestParameters1(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var parameterValues = new
                {
                    Val1 = 1,
                    Val2 = 2,
                    Val3 = 3,
                    Val4 = 34,
                    Val5 = 5
                };

                var table = GetTarget(db);

                table
                    .Merge()
                    .Using(GetSource2(db)
                        .Where(x => x.OtherId != parameterValues.Val5)
                        .Select(x => new
                        {
                            Id = x.OtherId,
                            Field1 = x.OtherField1,
                            Field2 = x.OtherField2,
                            Field3 = x.OtherField3,
                            Field4 = x.OtherField4,
                            Field5 = x.OtherField5,
                            Field7 = parameterValues.Val2
                        }))
                    .On((t, s) => t.Id == s.Id || t.Id == parameterValues.Val4)
                    .InsertWhenNotMatchedAnd(
                        s => s.Field7 == parameterValues.Val1 + s.Id,
                        s => new TestMapping1()
                        {
                            Id = s.Id + parameterValues.Val5,
                            Field1 = s.Field1
                        })
                    .UpdateWhenMatchedAnd(
                        (t, s) => s.Id == parameterValues.Val3,
                        (t, s) => new TestMapping1()
                        {
                            Field4 = parameterValues.Val5
                        })
                    .DeleteWhenMatchedAnd((t, s) => t.Field3 == parameterValues.Val2 + 123)
                    .Merge();

                var parametersCount = 8;

                switch (context)
                {
                    case DB2iSeriesProviderName.DB2iSeries_DB2Connect:
                    case DB2iSeriesProviderName.DB2:
                    case DB2iSeriesProviderName.DB2_GAS:
                    case DB2iSeriesProviderName.DB2_73:
                    case DB2iSeriesProviderName.DB2_73_GAS:
						parametersCount = 5;
                        break;
                    case ProviderName.DB2:
                        parametersCount = 1;
                        break;
                    case ProviderName.Firebird:
                        parametersCount = 4;
                        break;
                }

                Assert.AreEqual(parametersCount, db.LastQuery.Count(x => x == GetParameterToken(context)));
            }
        }

        // ASE: just fails
        [Test, MergeDataContextSource(ProviderName.Oracle, ProviderName.OracleManaged, ProviderName.OracleNative,
            ProviderName.Sybase, ProviderName.Informix, ProviderName.SapHana, ProviderName.Firebird)]
        public void TestParameters3(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var parameterValues = new
                {
                    Val1 = 1,
                    Val2 = 2,
                    Val3 = 3,
                    Val4 = 4,
                    Val5 = 5
                };

                var table = GetTarget(db);

                table
                    .Merge()
                    .Using(GetSource2(db)
                        .Where(x => x.OtherId != parameterValues.Val5)
                        .Select(x => new
                        {
                            Id = x.OtherId,
                            Field1 = x.OtherField1,
                            Field2 = x.OtherField2,
                            Field3 = x.OtherField3,
                            Field4 = x.OtherField4,
                            Field5 = x.OtherField5,
                            Field7 = parameterValues.Val2
                        }))
                    .On((t, s) => t.Id == s.Id)
                    .InsertWhenNotMatchedAnd(
                        s => s.Field7 == parameterValues.Val1 + s.Id,
                        s => new TestMapping1()
                        {
                            Id = s.Id + parameterValues.Val5,
                            Field1 = s.Field1
                        })
                    .UpdateWhenMatchedAnd(
                        (t, s) => s.Id == parameterValues.Val3,
                        (t, s) => new TestMapping1()
                        {
                            Field4 = parameterValues.Val5
                        })
                    .DeleteWhenMatchedAnd((t, s) => t.Field3 != parameterValues.Val2)
                    .Merge();

                var parametersCount = 7;

                switch (context)
                {
                    case DB2iSeriesProviderName.DB2iSeries_DB2Connect:
                    case DB2iSeriesProviderName.DB2:
                    case DB2iSeriesProviderName.DB2_GAS:
                    case DB2iSeriesProviderName.DB2_73:
                    case DB2iSeriesProviderName.DB2_73_GAS:
						parametersCount = 4;
                        break;
                    case ProviderName.DB2:
                        parametersCount = 1;
                        break;
                    case ProviderName.Firebird:
                        parametersCount = 3;
                        break;
                }

                Assert.AreEqual(parametersCount, db.LastQuery.Count(x => x == GetParameterToken(context)));
            }
        }

        [Test, MergeDataContextSource]
        public void TestParametersInMatchCondition(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var param = 4;

                var table = GetTarget(db);

                var rows = table
                    .Merge()
                    .Using(GetSource1(db))
                    .On((t, s) => t.Id == s.Id && t.Id == param)
                    .UpdateWhenMatched()
                    .Merge();

                AssertRowCount(1, rows, context);

                var paramcount = 1;
                if (context == ProviderName.DB2 || context == ProviderName.Informix)
                    paramcount = 0;

                Assert.AreEqual(paramcount, db.LastQuery.Count(x => x == GetParameterToken(context)));
            }
        }

        private static char GetParameterToken(string context)
        {
            switch (context)
            {
                case ProviderName.Informix:
                    return '?';
                case ProviderName.SapHana:
                case ProviderName.Oracle:
                case ProviderName.OracleManaged:
                case ProviderName.OracleNative:
                    return ':';
            }

            return '@';
        }

        [Test, MergeDataContextSource(ProviderName.Informix, ProviderName.SapHana, ProviderName.Firebird)]
        public void TestParametersInUpdateCondition(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var param = 4;

                var table = GetTarget(db);

                var rows = table
                    .Merge()
                    .Using(GetSource1(db))
                    .OnTargetKey()
                    .UpdateWhenMatchedAnd((t, s) => t.Id == param)
                    .Merge();

                AssertRowCount(1, rows, context);

                var paramcount = 1;
                if (context == ProviderName.DB2)
                    paramcount = 0;

                Assert.AreEqual(paramcount, db.LastQuery.Count(x => x == GetParameterToken(context)));
            }
        }

        [Test, MergeDataContextSource(ProviderName.Informix, ProviderName.SapHana, ProviderName.Firebird)]
        public void TestParametersInInsertCondition(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var param = 5;

                var table = GetTarget(db);

                var rows = table
                    .Merge()
                    .Using(GetSource1(db))
                    .OnTargetKey()
                    .InsertWhenNotMatchedAnd(s => s.Id == param)
                    .Merge();

                AssertRowCount(1, rows, context);

                Assert.AreEqual(1, db.LastQuery.Count(x => x == GetParameterToken(context)));
            }
        }

        [Test, MergeDataContextSource(ProviderName.Oracle, ProviderName.OracleManaged, ProviderName.OracleNative,
            ProviderName.Sybase, ProviderName.Informix, ProviderName.SapHana, ProviderName.Firebird)]
        public void TestParametersInDeleteCondition(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var param = 4;

                var table = GetTarget(db);

                var rows = table
                    .Merge()
                    .Using(GetSource1(db))
                    .OnTargetKey()
                    .DeleteWhenMatchedAnd((t, s) => s.Id == param)
                    .Merge();

                AssertRowCount(1, rows, context);

                var paramcount = 1;
                if (context == ProviderName.DB2)
                    paramcount = 0;

                Assert.AreEqual(paramcount, db.LastQuery.Count(x => x == GetParameterToken(context)));
            }
        }

        // FB, INFORMIX: supports this parameter, but for now we disable all parameters in source for them
        [Test, MergeDataContextSource(ProviderName.Firebird, ProviderName.Informix)]
        public void TestParametersInSourceFilter(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var param = 3;

                var table = GetTarget(db);

                var rows = table
                    .Merge()
                    .Using(GetSource1(db).Where(x => x.Id == param))
                    .OnTargetKey()
                    .UpdateWhenMatched()
                    .Merge();

                AssertRowCount(1, rows, context);
                Assert.AreEqual(1, db.LastQuery.Count(x => x == GetParameterToken(context)));
            }
        }

        // FB, INFORMIX, Oracle: doesn't support parameters in source select list
        [Test, MergeDataContextSource(
            ProviderName.Firebird, ProviderName.Informix,
            ProviderName.Oracle, ProviderName.OracleNative, ProviderName.OracleManaged)]
        public void TestParametersInSourceSelect(string context)
        {
            using (var db = new TestDataConnection(context))
            {
                PrepareData(db);

                var param = 3;

                var table = GetTarget(db);

                var rows = table
                    .Merge()
                    .Using(GetSource1(db).Select(x => new { x.Id, Val = param }))
                    .On((t, s) => t.Id == s.Id && t.Id == s.Val)
                    .UpdateWhenMatched((t, s) => new TestMapping1()
                    {
                        Field1 = s.Val + 111
                    })
                    .Merge();

                AssertRowCount(1, rows, context);

                var paramcount = 1;
                if (context.Contains("iSeries"))
                    paramcount = 0;

                Assert.AreEqual(paramcount, db.LastQuery.Count(x => x == GetParameterToken(context)));

                var result = GetTarget(db).Where(x => x.Id == 3).ToList();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(114, result[0].Field1);
            }
        }
    }
}
