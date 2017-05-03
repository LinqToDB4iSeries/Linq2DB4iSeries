using System;
using System.Linq;

using LinqToDB;

using NUnit.Framework;

namespace Tests.Linq
{
	using Model;

	[TestFixture]
	public class ExtensionTests : TestBase
	{
		public class ParenTable
		{
			public int ParentID;
			public int? Value1;
		}

		//[Test, DataContextSource]
		//public void TableName(string context)
		//{
		//	using (var db = GetDataContext(context))
		//		db.GetTable<ParenTable>().TableName("Parent").ToList();
		//}

		//[Test, DataContextSource]
		//public void DatabaseName(string context)
		//{
		//	using (var db = GetDataContext(context))
		//		db.GetTable<Parent>().DatabaseName("TestData").ToList();
		//}

		[Test, DataContextSource]
		public void SchemaName(string context)
		{
			using (var db = GetDataContext(context))
			{
				var result = db.GetTable<Parent>().SchemaName("WIBBLE").ToList();

			}
		}

		//[Test, DataContextSource]
		//public void AllNames(string context)
		//{
		//	using (var db = GetDataContext(context))
		//		db.GetTable<ParenTable>()
		//			.DatabaseName("TestData")
		//			.SchemaName("dbo")
		//			.TableName("Parent")
		//			.ToList();
		//}
	}
}
