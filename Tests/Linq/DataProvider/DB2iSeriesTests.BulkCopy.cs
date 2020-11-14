using LinqToDB;
using LinqToDB.Data;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq;
using Tests.Model;

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		void BulkCopyTest_AccessClient(string context, BulkCopyType bulkCopyType, int maxSize, int batchSize)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					conn.BulkCopy(
						new BulkCopyOptions
						{
							MaxBatchSize = maxSize,
							BulkCopyType = bulkCopyType,
							NotifyAfter = 10000,
							RowsCopiedCallback = copied => Debug.WriteLine(copied.RowsCopied)
						},
						Enumerable.Range(0, batchSize).Select(n =>
							new ALLTYPE
							{
								ID = 2000 + n,
								BIGINTDATATYPE = 3000 + n,
								INTDATATYPE = 4000 + n,
								SMALLINTDATATYPE = (short)(5000 + n),
								DECIMALDATATYPE = 6000 + n,
								DECFLOAT16DATATYPE = 7000 + n,
								DECFLOAT34DATATYPE = 7000 + n,
								REALDATATYPE = 8000 + n,
								DOUBLEDATATYPE = 9000 + n,
								CHARDATATYPE = 'A',
								VARCHARDATATYPE = "",
								CLOBDATATYPE = "123",
								DBCLOBDATATYPE = "αβγ",
								BINARYDATATYPE = new byte[] { 1, 2, 3 },
								VARBINARYDATATYPE = new byte[] { 1, 2, 3 },
								BLOBDATATYPE = new byte[] { 1, 2, 3 },
								GRAPHICDATATYPE = "αβγ",
								VARGRAPHICDATATYPE = "βγδ",
								DATEDATATYPE = DateTime.Now.Date,
								TIMEDATATYPE = TimeSpan.FromSeconds(10),
								TIMESTAMPDATATYPE = DateTime.Now,
								XMLDATATYPE = "<root><element strattr=\"strvalue\" intattr=\"12345\"/></root>"
							}));
				}
				catch (Exception e)
				{
					Assert.Fail(e.Message);
				}
				finally
				{
					conn.GetTable<ALLTYPE>().Delete(p => p.DECIMALDATATYPE >= 6000);
				}
			}
		}

		void BulkCopyTest_DB2Connect(string context, BulkCopyType bulkCopyType, int maxSize, int batchSize)
		{
			using (var conn = new DataConnection(context))
			{
				try
				{
					conn.BulkCopy(
						new BulkCopyOptions
						{
							MaxBatchSize = maxSize,
							BulkCopyType = bulkCopyType,
							NotifyAfter = 10000,
							RowsCopiedCallback = copied => Debug.WriteLine(copied.RowsCopied)
						},
						Enumerable.Range(0, batchSize).Select(n =>
							new ALLTYPE2
							{
								ID = 2000 + n,
								BIGINTDATATYPE = 3000 + n,
								INTDATATYPE = 4000 + n,
								SMALLINTDATATYPE = (short)(5000 + n),
								DECIMALDATATYPE = 6000 + n,
								DECFLOAT16DATATYPE = 7000 + n,
								DECFLOAT34DATATYPE = 7000 + n,
								REALDATATYPE = 8000 + n,
								DOUBLEDATATYPE = 9000 + n,
								CHARDATATYPE = 'A',
								VARCHARDATATYPE = "123",
								BINARYDATATYPE = new byte[] { 1, 2, 3 },
								VARBINARYDATATYPE = new byte[] { 1, 2, 3 },
								GRAPHICDATATYPE = "αβγ",
								VARGRAPHICDATATYPE = "βγδ",
								DATEDATATYPE = DateTime.Now.Date,
								TIMEDATATYPE = TimeSpan.FromSeconds(10),
								TIMESTAMPDATATYPE = DateTime.Now,
							}));
				}
				catch (Exception e)
				{
					Assert.Fail(e.Message);
				}
				finally
				{
					conn.GetTable<ALLTYPE>().Delete(p => p.DECIMALDATATYPE >= 6000);
				}
			}
		}

		[Test]
		public void BulkCopyMultipleRows([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			BulkCopyTest_AccessClient(context, BulkCopyType.MultipleRows, 5000, 100);
		}

		[Test]
		public void BulkCopyProviderSpecific([IncludeDataSources(TestProvNameDb2i.All_AccessClient, TestProvNameDb2i.All_DB2Connect)] string context)
		{
			if (TestProvNameDb2i.IsiSeriesAccessClient(context))
				BulkCopyTest_DB2Connect(context, BulkCopyType.ProviderSpecific, 50000, 100001);
			if (TestProvNameDb2i.IsiSeriesDB2Connect(context))
				BulkCopyTest_AccessClient(context, BulkCopyType.ProviderSpecific, 50000, 100001);
		}

		[Test]
		public void BulkCopyLinqTypesMultipleRows([IncludeDataSources(TestProvNameDb2i.All)] string context)
		{
			using (var db = new DataConnection(context))
			{
				try
				{
					db.BulkCopy(
						new BulkCopyOptions { BulkCopyType = BulkCopyType.MultipleRows },
						Enumerable.Range(0, 10).Select(n =>
							new LinqDataTypes
							{
								ID = 4000 + n,
								MoneyValue = 1000m + n,
								DateTimeValue = new DateTime(2001, 1, 11, 1, 11, 21, 100),
								BoolValue = true,
								GuidValue = Guid.NewGuid(),
								SmallIntValue = (short)n
							}
						));
				}
				catch (Exception e)
				{
					Assert.Fail(e.ToString());
				}
				finally
				{
					db.GetTable<LinqDataTypes>().Delete(p => p.ID >= 4000);
				}
			}
		}
	}
}
