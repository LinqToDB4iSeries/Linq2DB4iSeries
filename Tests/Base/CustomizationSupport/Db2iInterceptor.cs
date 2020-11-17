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
			//Filter 5.4 providers from Merge and InsertOrUpdate annotated tests.
			if (	dataSourcesAttribute.GetType().Name == "InsertOrUpdateDataSourcesAttribute"
				||  dataSourcesAttribute.GetType().Name == "MergeDataContextSourceAttribute"
				||  dataSourcesAttribute.GetType().Name == "IdentityInsertMergeDataContextSourceAttribute")
			{
				return contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
			}
			
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
				//Tests passing provider specific parameter types - Generic linq2db test - Not applicable
				case ("MergeTests", "TestParametersInListSourceProperty"):
				//Tests active issue with DB2 family ordering NULL last by default - Not applicable
				case ("MergeTests", "SortedMergeResultsIssue"):
				//Too many cases in code - Copied to custom tests
				case ("CharTypesTests", _):
				//Too many changes and cases - Copied to custom tests
				case ("MergeTests", "TestTypesInsertByMerge"):
					return Enumerable.Empty<string>();
				//Merge related tests
				case ("OldMergeTests", "Merge"):
				case ("OldMergeTests", "MergeWithEmptySource"):
					return contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
				//Access client throws a different exception so it is excluded
				case ("DataContextTests", "ProviderConnectionStringConstructorTest2"):
				
					return contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_AccessClient));
			}
			
			return contexts;
		}

		private static (string className, string methodName) ExtractMethod(IMethodInfo testMethod)
			=> (testMethod.TypeInfo.Name, testMethod.Name);
	}
}
