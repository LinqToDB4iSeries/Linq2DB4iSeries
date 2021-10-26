//Edited tests copied from linq2db 

using LinqToDB.Data;
using NUnit.Framework;
using System.Linq;

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		[Test]
		public void SchemaProvider([IncludeDataSources(TestProvNameDb2i.All_73, TestProvNameDb2i.All_74)] string context)
		{
			using (var conn = new DataConnection(context))
			{
				var sp = conn.DataProvider.GetSchemaProvider();
				var dbSchema = sp.GetSchema(conn);

				var table = dbSchema.Tables.Single(t => t.IsDefaultSchema && t.TableName == "ALLTYPES");

				if (TestProvNameDb2i.IsiSeries(context))
				{
					var binaryType = TestProvNameDb2i.IsiSeriesAccessClient(context) ? "BINARY(20)" : "BINARY";

					Assert.That(table.Columns.Single(c => c.ColumnName == "BINARYDATATYPE").ColumnType, Is.EqualTo(binaryType));
					Assert.That(table.Columns.Single(c => c.ColumnName == "VARBINARYDATATYPE").ColumnType, Is.EqualTo("VARBIN"));
				}
				else
				{
					Assert.That(table.Columns.Single(c => c.ColumnName == "BINARYDATATYPE").ColumnType, Is.EqualTo("CHAR (5) FOR BIT DATA"));
					Assert.That(table.Columns.Single(c => c.ColumnName == "VARBINARYDATATYPE").ColumnType, Is.EqualTo("VARCHAR (5) FOR BIT DATA"));
				}
			}
		}
	}
}
