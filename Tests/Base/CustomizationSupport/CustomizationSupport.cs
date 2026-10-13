// Provider configurations can share the same IBM i database, so all tests must run serially.
// Zero workers also prevents installation of the resource-lane parallel dispatcher.
using NUnit.Framework;

[assembly: NonParallelizable]
[assembly: LevelOfParallelism(0)]

namespace Tests
{
	public static class CustomizationSupport
	{
		//Replace this instance with a custom implementation to override default behaviour
		public static readonly CustomizationSupportInterceptor Interceptor = new Db2iInterceptor();

		public static void Init()
		{
			LinqToDB.DataProvider.DB2iSeries.DB2iSeriesTools.RegisterProviderDetector();
		}
	}
}
