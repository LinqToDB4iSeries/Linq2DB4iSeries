using NUnit.Framework.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace Tests
{
	public class Db2iInterceptor : CustomizationSupportInterceptor
	{
		public override IEnumerable<string> GetSupportedProviders(IEnumerable<string> providers)
		{
			return TestProvNameDb2i.GetAll();
		}

		public override IEnumerable<string> InterceptDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IEnumerable<string> contexts)
		{
			return contexts.Concat(TestProvNameDb2i.GetProviders(contexts));
		}

		public override IEnumerable<string> InterceptTestDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IMethodInfo testMethod, IEnumerable<string> contexts)
		{
			var test = ExtractMethod(testMethod);

			//Filter out specific tests
			switch (ExtractMethod(testMethod))
			{
				//Test targets DB2 provider
				case ("SchemaProviderTests", "DB2Test"):
				case ("MiniProfilerTests", "TestDB2"):
				case ("DB2Tests", _):
				//Tests have internal logic based on BulkCopyType - Copied to custom tests
				case ("BulkCopyTests", "KeepIdentity_SkipOnInsertTrue"):
				case ("BulkCopyTests", "KeepIdentity_SkipOnInsertFalse"):
				//Tests have property have space - Copied to custom tests
				case ("DynamicColumnsTests", "SqlPropertyNoStoreNonIdentifier"):
				case ("DynamicColumnsTests", "SqlPropertyNoStoreNonIdentifierGrouping"):
				//Valid for DB2 but not DB2i - Test copied to custom
				case ("DropTableTests", "DropSpecificDatabaseTableTest"):
				//Test copied to custom to reduce default source row number
				case ("MergeTests", "BigSource"):
				//Tests passing provider specific parameter types - Generic linq2db test - Not applicable
				case ("MergeTests", "TestParametersInListSourceProperty"):
				//Tests active issue with DB2 family ordering NULL last by default - Not applicable
				case ("MergeTests", "SortedMergeResultsIssue"):
				//Tests are not applicable for Db2i (MERGE from/to CTE)
				case ("MergeTests", "MergeFromCte"):
				case ("MergeTests", "MergeIntoCte"):
				case ("MergeTests", "MergeUsingCteJoin"):
				case ("MergeTests", "MergeUsingCteWhere"):
				//Too many cases in code - Copied to custom tests
				case ("CharTypesTests", _):
				//Too many changes and cases - Copied to custom tests
				case ("MergeTests", "TestTypesInsertByMerge"):
				case ("MergeTests", "TestMergeTypes"):
				case ("MergeTests", "TestDB2NullsInSource"):
				case ("Issue681Tests", "TestTableFQN"):
				//Query incorrect for DB2i - Copied to custom tests
				case ("DataConnectionTests", "EnumExecuteScalarTest"):
				//Case valid for DB2 but not for DB2i
				case ("Issue792Tests", "TestWithTransactionThrowsFromProvider"):
				case ("Issue3148Tests", "TestDefaultExpression_09"):
				//Data not valid for DB2i
				case ("Issue1287Tests", _):
				case ("TableOptionsTests", "CheckExistenceTest"):
				case ("TableOptionsTests", "CreateIfNotExistsTest"):
				case ("TableOptionsTests", "CreateTempIfNotExistsTest"):
				case ("TableOptionsTests", "DB2TableOptionsTest"):
				case ("TableOptionsTests", "FluentMappingTest"):
				case ("TableOptionsTests", "IsTemporaryFlagTest"):
				case ("TableOptionsTests", "IsTemporaryMethodTest"):
				case ("TableOptionsTests", "IsTemporaryMethodTest2"):
				case ("TableOptionsTests", "IsTemporaryMethodTest3"):
				case ("TableOptionsTests", "IsTemporaryOptionAsyncTest"):
				case ("TableOptionsTests", "IsTemporaryOptionTest"):
				case ("TableOptionsTests", "TableOptionsMethodTest"):
				//Implicit transactions do not function properly in .NET
				case ("DataConnectionTests", "TestDisposeFlagCloning962Test1"):
				case ("DataConnectionTests", "TestDisposeFlagCloning962Test2"):
				//Query contains invalid keyword permission
				case ("Issue825Tests", "Test"):
				//Test for unsupported WCF feature
				case ("AsyncTests", "Test"):
				case ("AsyncTests", "Test1"):
				case ("AsyncTests", "TestForEach"):
				//Invalid query in test
				case ("DataExtensionsTests", "TestDataParameterMapping1"):
				case ("DataExtensionsTests", "TestObject3"):
				case ("DataExtensionsTests", "TestObject4"):
				case ("DataExtensionsTests", "TestObject5"):
				case ("DataExtensionsTests", "TestObject6"):
				//There is no collation in DB2i
				case ("SqlExtensionsTests", "TestSqlCollate1"):
				case ("SqlExtensionsTests", "TestSqlCollate2"):
				//Unsupported table options
				case ("CreateTempTableTests", "CreateTable_NoDisposeError"):
				case ("CreateTempTableTests", "CreateTableAsyncCanceled"):
				case ("CreateTempTableTests", "CreateTableAsyncCanceled2"):
				case ("CreateTempTableTests", "CreateTable_NoDisposeErrorAsync"):
				case ("CreateTempTableTests", "CreateTempTableWithPrimaryKey"):
				case ("CreateTempTableTests", "InsertIntoTempTableWithPrimaryKey"):
				// GUIDs are serialized in lower case
				case ("ConvertTests", "GuidToString"):
				// Recursive CTE expression defined in test is not supported
				case ("CteTests", "Issue3357_RecordClass_DB2"):
				case ("CteTests", "Issue3357_RecordLikeClass_DB2"):
				//UpdateRow / UpdateRowLiteral not supported
				case ("SqlRowTests", "UpdateRowLiteral"):
				case ("SqlRowTests", "UpdateRowSelect"):
					return Enumerable.Empty<string>();

				//Access client throws a different exception so it is excluded
				case ("DataContextTests", "ProviderConnectionStringConstructorTest2"):
					return contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_AccessClient));

				//OleDb doesn't support inline comments so tags are not supported (fails with PREPARE STATEMENT error)
				case ("TagTests", _):
				//OleDb doesn't support inline comments so test that perform comment assertions are not supported
				case ("QueryNameTests", "TableTest"):
				case ("QueryNameTests", "FromTest"):
				case ("QueryNameTests", "MainInlineTest"):
					return contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_OleDb));

				//LAG returns numeric instead of timestamp prior to 7.4
				case ("AnalyticTests", "Issue1799Test1"):
				case ("AnalyticTests", "Issue1799Test2"):
					return contexts.Intersect(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_74));
			}

			return contexts;
		}

		public override char GetParameterToken(char token, string context)
		{
			if (TestProvNameDb2i.IsiSeriesOleDb(context) || TestProvNameDb2i.IsiSeriesODBC(context))
				return '?';
			else
				return '@';
		}

		public override CreateDataScript? InterceptCreateData(string context)
		{
			var script = context.Contains("GAS") ? "DB2iSeriesGAS" : "DB2iSeries";
			return new CreateDataScript(context, "\nGO\n", script);
		}

		public override string[]? InterceptResetPersonIdentity(string context, int lastValue)
		{
			return new[] { $"ALTER TABLE Person ALTER COLUMN PersonID RESTART WITH {lastValue + 1}" };
		}

		public override string[]? InterceptResetAllTypesIdentity(string context, int lastValue, int keepIdentityLastValue)
		{
			return new[]
			{
				$"ALTER TABLE AllTypes ALTER COLUMN ID RESTART WITH {lastValue + 1}",
				$"ALTER TABLE KeepIdentityTest ALTER COLUMN ID RESTART WITH {keepIdentityLastValue + 1}",
			};
		}

		public override bool IsCaseSensitiveComparison(string context) => true;
		public override bool IsCaseSensitiveDB(string context) => true;
		public override bool IsCollatedTableConfigured(string context) => false;
	}
}
