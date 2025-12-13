using NUnit.Framework;
using System.Reflection;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void BigSource([MergeDataContextSource] string context)
		{
			var batchSize = 500;

			typeof(xUpdate.MergeTests).GetMethod("RunTest", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!
				.Invoke(new xUpdate.MergeTests(), new object[] { context, batchSize });
		}
	}
}
