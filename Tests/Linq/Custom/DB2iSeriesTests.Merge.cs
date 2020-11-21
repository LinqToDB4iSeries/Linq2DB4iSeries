using LinqToDB.Data;
using NUnit.Framework;
using System.Linq;
using System.Reflection;

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void BigSource([Tests.xUpdate.MergeTests.MergeDataContextSource] string context)
		{
			var batchSize = 500;

			typeof(Tests.xUpdate.MergeTests).GetMethod("RunTest", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
				.Invoke(new Tests.xUpdate.MergeTests(), new object[] { context, batchSize });
		}
	}
}
