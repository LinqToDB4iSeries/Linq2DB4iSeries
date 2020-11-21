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
				//Tests have internal logic based on BulkCopyType - Copied to custom tests
				case ("BulkCopyTests", "KeepIdentity_SkipOnInsertTrue"):
				case ("BulkCopyTests", "KeepIdentity_SkipOnInsertFalse"):
				//Tests have property have space - Copied to custom tests
				case ("DynamicColumnsTests", "SqlPropertyNoStoreNonIdentifier"):
				case ("DynamicColumnsTests", "SqlPropertyNoStoreNonIdentifierGrouping"):
				//Test copied to custom to reduce default source row number
				case ("MergeTests", "BigSource"):
				//Tests passing provider specific parameter types - Generic linq2db test - Not applicable
				case ("MergeTests", "TestParametersInListSourceProperty"):
				//Tests active issue with DB2 family ordering NULL last by default - Not applicable
				case ("MergeTests", "SortedMergeResultsIssue"):
				//Too many cases in code - Copied to custom tests
				case ("CharTypesTests", _):
				//Too many changes and cases - Copied to custom tests
				case ("MergeTests", "TestTypesInsertByMerge"):
				case ("MergeTests", "TestMergeTypes"):
				case ("MergeTests", "TestDB2NullsInSource"):
				//Case valid for DB2 but not for DB2i
				case ("Issue792Tests", "TestWithTransactionThrowsFromProvider"):
				//Data not valid for DB2i
				case ("Issue1287Tests", _):
				//Query contains invalid keyword permission
				case ("Issue825Tests", "Test"):
					return Enumerable.Empty<string>();
				
				//Access client throws a different exception so it is excluded
				case ("DataContextTests", "ProviderConnectionStringConstructorTest2"):
					return contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_AccessClient));
			}

			//Filter 5.4 providers from Merge and InsertOrUpdate annotated tests.
			if (dataSourcesAttribute.GetType().Name == "InsertOrUpdateDataSourcesAttribute"
				|| dataSourcesAttribute.GetType().Name == "MergeDataContextSourceAttribute"
				|| dataSourcesAttribute.GetType().Name == "IdentityInsertMergeDataContextSourceAttribute"
				|| test.className == "OldMergeTests")
			{
				contexts = contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
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
			if (TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54).Contains(context))
				script += "54";
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
	}
}
