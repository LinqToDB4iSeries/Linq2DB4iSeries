using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using NUnit.Framework;
using System;
using System.Linq;

#nullable disable

namespace Tests.DataProvider
{
	[TestFixture]
	public partial class DB2iSeriesTests : TestBase
	{
		[Test]
		public void Issue825_Test([DataSources] string context)
		{
			using (var db = GetDataContext(context))
			{
				var userId = 32;
				var childId = 32;

				//Configuration.Linq.OptimizeJoins = false;

				var query = db.GetTable<UserTests.Issue825Tests.Parent825>()
					.Where(p => p.ParentPermissions.Any(perm => perm.UserId == userId))
					.SelectMany(parent => parent.Childs)
					.Where(child => child.Id == childId)
					.Select(child => child.Parent);

				var result = query.ToList();

				Assert.AreEqual(1, result.Count);
				Assert.AreEqual(3, result[0].Id);
			}
		}

		[Table]
		class TestTrun
		{
			[Column, PrimaryKey] public int ID;
			[Column] public decimal Field1;
		}
	}
}
