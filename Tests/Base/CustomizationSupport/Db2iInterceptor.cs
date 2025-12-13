using LinqToDB.Extensions;
using NUnit.Framework.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Tests
{
	public interface ITestInterceptor
	{
		IEnumerable<string> InterceptDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IEnumerable<string> contexts);
		IEnumerable<string> InterceptTestDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IMethodInfo testMethod, IEnumerable<string> contexts);
	}

	public class Db2iInterceptor : CustomizationSupportInterceptor
	{
		private List<ITestInterceptor> interceptors;

		public Db2iInterceptor()
		{
			interceptors = AppDomain.CurrentDomain.GetAssemblies()
				.SelectMany(x => 
					x.ExportedTypes.Where(x => x.GetInterfaces().Contains(typeof(ITestInterceptor))))
				.Select(x => (ITestInterceptor)Activator.CreateInstance(x)!)
				.ToList();
		}

		public override IEnumerable<string> GetSupportedProviders(IEnumerable<string> providers)
		{
			return TestProvNameDb2i.GetAll();
		}

		public override IEnumerable<string> InterceptDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IEnumerable<string> contexts)
		{
			return interceptors.Aggregate(
					contexts.Concat(TestProvNameDb2i.GetProviders(contexts)),
					(acc, cur) => cur.InterceptDataSources(dataSourcesAttribute, acc));
		}

		public override IEnumerable<string> InterceptTestDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IMethodInfo testMethod, IEnumerable<string> contexts)
		{
			// Skip tests marked as ActiveIssue for all providers or for DB2
			if (testMethod.MethodInfo.GetAttributes<ActiveIssueAttribute>(false)
				.Any(x => x.Configurations is null ||
					x.Configurations.Length == 0 ||
					x.Configurations?.Intersect(TestProvName.AllDB2.Split(',')).Any() == true))
				return [];
			
			return interceptors.Aggregate(
					contexts,
					(acc, cur) => cur.InterceptTestDataSources(dataSourcesAttribute, testMethod, acc));
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
