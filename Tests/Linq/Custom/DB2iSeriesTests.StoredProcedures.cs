using NUnit.Framework;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using System;
using System.Data;
using System.Linq;
using System.ServiceModel;
using Tests.Model;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests : TestBase
	{
		[Test]
		public void TestStoredProcedureOutputIntParameters([DataSources] string context)
		{
			using (var db = GetDataConnection(context))
			{
				var input = DataParameter.Int32("input", 1);
				var output1 = new DataParameter("output1", null, DataType.Int32) { Direction = ParameterDirection.Output };
				var output2 = new DataParameter("output2", null, DataType.Int32) { Direction = ParameterDirection.Output };
				var result = db.ExecuteProc("ExecuteProcInt2Parameters", input, output1, output2);
				Assert.AreEqual(2, output1.Value);
				Assert.AreEqual(3, output2.Value);
				Assert.AreEqual(TestProvNameDb2i.IsiSeriesODBC(context) ? 0 : -1, result);
			}
		}
	}
}
