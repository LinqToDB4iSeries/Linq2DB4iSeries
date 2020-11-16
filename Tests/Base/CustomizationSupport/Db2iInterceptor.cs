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
			if (	dataSourcesAttribute.GetType().Name == "InsertOrUpdateDataSourcesAttribute"
				||  dataSourcesAttribute.GetType().Name == "MergeDataContextSourceAttribute"
				||  dataSourcesAttribute.GetType().Name == "IdentityInsertMergeDataContextSourceAttribute")
			{
				return contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
			}
			
			//Filter out specific tests
			switch (ExtractMethod(testMethod))
			{
				case ("SchemaProviderTests", "DB2Test"):
					return Enumerable.Empty<string>();
			}
			
			return contexts;
		}


		public override IEnumerable<string> GetInsertOrUpdateUnsupportedProviders(IEnumerable<string> providers)
		{
			return providers.Concat(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
		}

		public override IEnumerable<string> GetMergeUnsupportedProviders(IEnumerable<string> providers)
		{
			return providers.Concat(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
		}

		public override IEnumerable<string> GetIdentityInsertMergeSupportedProviders(IEnumerable<string> providers)
		{
			return base.GetIdentityInsertMergeSupportedProviders(providers);
		}

		private static (string className, string methodName) ExtractMethod(IMethodInfo testMethod)
			=> (testMethod.TypeInfo.Name, testMethod.Name);
	}


	public static class ProviderSupport
	{
		public static IEnumerable<string> GetSupportedProviders(IEnumerable<string> providers)
		{
			return TestProvNameDb2i.GetAll();
		}

		public static IEnumerable<string> GetInsertOrUpdateUnsupportedProviders(IEnumerable<string> providers)
		{
			return providers.Concat(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
		}

		public static IEnumerable<string> GetMergeUnsupportedProviders(IEnumerable<string> providers)
		{
			return providers.Concat(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_54));
		}

		public static IEnumerable<string> GetIdentityInsertMergeSupportedProviders(IEnumerable<string> providers)
		{
			return providers;
		}
	}

	public static class ProviderTestSupport
	{
		public static IEnumerable<string> InterceptSupportedProviders(IEnumerable<string> providers)
		{
			return providers.Concat(TestProvNameDb2i.GetProviders(providers));
		}

		public static IEnumerable<string> InterceptSupportedTestProviders(IMethodInfo testMethod, IEnumerable<string> providers)
		{
			//Filter out specific tests
			if (testMethod.TypeInfo.Name == "SchemaProviderTests"
				&& testMethod.Name == "DB2Test")
				return Enumerable.Empty<string>();

			return providers;
		}
	}
}
