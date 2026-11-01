using LinqToDB;
using LinqToDB.Mapping;
using NUnit.Framework;
using Shouldly;
using System.Linq;

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		sealed class WideUnionDecimalRow
		{
			[Column(DataType = DataType.Decimal, Precision = 18, Scale = 2)]
			public decimal? Value { get; set; }
		}

		[Test]
		public void WideDecimalUnionPreservesNumericOperations([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using var db = GetDataContext(context);
			using var table = db.CreateLocalTable(new[]
			{
				new WideUnionDecimalRow { Value = 12345678901234.50m },
				new WideUnionDecimalRow { Value = 2m },
				new WideUnionDecimalRow { Value = null },
			});

			var query = table.Select(r => Sql.Abs(r.Value))
				.Union(table.Select(r => (decimal?)1.00000000000000005m));

			query.OrderBy(v => v).ToArray().ShouldBe(new decimal?[]
			{
				1.00000000000000005m, 2m, 12345678901234.50m, null,
			});
			query.OrderByDescending(v => v).Skip(1).Take(2).ToArray()
				.ShouldBe(new decimal?[] { 12345678901234.50m, 2m });
		}
	}
}
