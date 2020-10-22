using System;
using System.Data;
using System.IO;
using System.Linq;

using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.Access;

using NUnit.Framework;
using Tests;
using Tests.Model;

// for unknown reason order doesn't help on ubuntu 18, so namespace were removed and class name changed to be first in
// sort order
[TestFixture]
[Category(TestCategory.Create)]
[Order(-1)]
// ReSharper disable once InconsistentNaming
// ReSharper disable once TestClassNameSuffixWarning
public class a_CreateData : TestBase
{
	static void RunScript(string configString, string divider, string name, Action<IDbConnection>? action = null, string? database = null)
	{
		Console.WriteLine("=== " + name + " === \n");

		var scriptFolder = Path.Combine(Path.GetFullPath("."), "Database", "Create Scripts");
		Console.WriteLine("Script folder exists: {1}; {0}", scriptFolder, Directory.Exists(scriptFolder));

		var sqlFileName = Path.GetFullPath(Path.Combine(scriptFolder, Path.ChangeExtension(name, "sql")));
		Console.WriteLine("Sql file exists: {1}; {0}", sqlFileName, File.Exists(sqlFileName));

		var text = File.ReadAllText(sqlFileName);

		while (true)
		{
			var idx = text.IndexOf("SKIP " + configString + " BEGIN");

			if (idx >= 0)
				text = text.Substring(0, idx) + text.Substring(text.IndexOf("SKIP " + configString + " END", idx));
			else
				break;
		}

		var cmds = text
			.Replace("{DBNAME}", database)
			.Replace("\r", "")
			.Replace(divider, "\x1")
			.Split('\x1')
			.Select(c => c.Trim())
			.Where(c => !string.IsNullOrEmpty(c))
			.ToArray();

		if (DataConnection.TraceSwitch.TraceInfo)
			Console.WriteLine("Commands count: {0}", cmds.Length);

		Exception? exception = null;

		using (var db = new TestDataConnection(configString))
		{
			//db.CommandTimeout = 20;

			foreach (var command in cmds)
			{
				try
				{
					if (DataConnection.TraceSwitch.TraceInfo)
						Console.WriteLine(command);

					if (configString == ProviderName.OracleNative || configString == TestProvName.Oracle11Native)
					{
						// we need this to avoid errors in trigger creation when native provider
						// recognize ":NEW" as parameter
						var cmd = db.CreateCommand();
						cmd.CommandText = command;
						((dynamic)cmd).BindByName = false;
						cmd.ExecuteNonQuery();
					}
					else
						db.Execute(command);

					if (DataConnection.TraceSwitch.TraceInfo)
						Console.WriteLine("\nOK\n");
				}
				catch (Exception ex)
				{
					if (DataConnection.TraceSwitch.TraceError)
					{
						if (!DataConnection.TraceSwitch.TraceInfo)
							Console.WriteLine(command);

						var isDrop =
							command.TrimStart().StartsWith("DROP") ||
							command.TrimStart().StartsWith("CALL DROP");

						Console.WriteLine(ex.Message);

						if (isDrop)
						{
							Console.WriteLine("\nnot too OK\n");
						}
						else
						{
							Console.WriteLine("\nFAILED\n");

							if (exception == null)
								exception = ex;
						}

					}
				}
			}

			if (exception != null)
				throw exception;

			if (DataConnection.TraceSwitch.TraceInfo)
				Console.WriteLine("\nBulkCopy LinqDataTypes\n");

			var options = new BulkCopyOptions();

			db.BulkCopy(
				options,
				new[]
				{
					new LinqDataTypes2 { ID =  1, MoneyValue =  1.11m, DateTimeValue = new DateTime(2001,  1,  11,  1, 11, 21, 100), BoolValue = true,  GuidValue = new Guid("ef129165-6ffe-4df9-bb6b-bb16e413c883"), SmallIntValue =  1, StringValue = null, BigIntValue = 1 },
					new LinqDataTypes2 { ID =  2, MoneyValue =  2.49m, DateTimeValue = new DateTime(2005,  5,  15,  5, 15, 25, 500), BoolValue = false, GuidValue = new Guid("bc663a61-7b40-4681-ac38-f9aaf55b706b"), SmallIntValue =  2, StringValue = "",   BigIntValue = 2 },
					new LinqDataTypes2 { ID =  3, MoneyValue =  3.99m, DateTimeValue = new DateTime(2009,  9,  19,  9, 19, 29,  90), BoolValue = true,  GuidValue = new Guid("d2f970c0-35ac-4987-9cd5-5badb1757436"), SmallIntValue =  3, StringValue = "1"  },
					new LinqDataTypes2 { ID =  4, MoneyValue =  4.50m, DateTimeValue = new DateTime(2009,  9,  20,  9, 19, 29,  90), BoolValue = false, GuidValue = new Guid("40932fdb-1543-4e4a-ac2c-ca371604fb4b"), SmallIntValue =  4, StringValue = "2"  },
					new LinqDataTypes2 { ID =  5, MoneyValue =  5.50m, DateTimeValue = new DateTime(2009,  9,  20,  9, 19, 29,  90), BoolValue = true,  GuidValue = new Guid("febe3eca-cb5f-40b2-ad39-2979d312afca"), SmallIntValue =  5, StringValue = "3"  },
					new LinqDataTypes2 { ID =  6, MoneyValue =  6.55m, DateTimeValue = new DateTime(2009,  9,  22,  9, 19, 29,  90), BoolValue = false, GuidValue = new Guid("8d3c5d1d-47db-4730-9fe7-968f6228a4c0"), SmallIntValue =  6, StringValue = "4"  },
					new LinqDataTypes2 { ID =  7, MoneyValue =  7.00m, DateTimeValue = new DateTime(2009,  9,  23,  9, 19, 29,  90), BoolValue = true,  GuidValue = new Guid("48094115-83af-46dd-a906-bff26ee21ee2"), SmallIntValue =  7, StringValue = "5"  },
					new LinqDataTypes2 { ID =  8, MoneyValue =  8.99m, DateTimeValue = new DateTime(2009,  9,  24,  9, 19, 29,  90), BoolValue = false, GuidValue = new Guid("c1139f1f-1335-4cd4-937e-92602f732dd3"), SmallIntValue =  8, StringValue = "6"  },
					new LinqDataTypes2 { ID =  9, MoneyValue =  9.63m, DateTimeValue = new DateTime(2009,  9,  25,  9, 19, 29,  90), BoolValue = true,  GuidValue = new Guid("46c5c512-3d4b-4cf7-b4e7-1de080789e5d"), SmallIntValue =  9, StringValue = "7"  },
					new LinqDataTypes2 { ID = 10, MoneyValue = 10.77m, DateTimeValue = new DateTime(2009,  9,  26,  9, 19, 29,  90), BoolValue = false, GuidValue = new Guid("61b2bc55-147f-4b40-93ed-a4aa83602fee"), SmallIntValue = 10, StringValue = "8"  },
					new LinqDataTypes2 { ID = 11, MoneyValue = 11.45m, DateTimeValue = new DateTime(2009,  9,  27,  0,  0,  0,   0), BoolValue = true,  GuidValue = new Guid("d3021d18-97f0-4dc0-98d0-f0c7df4a1230"), SmallIntValue = 11, StringValue = "9"  },
					new LinqDataTypes2 { ID = 12, MoneyValue = 11.45m, DateTimeValue = new DateTime(2012, 11,   7, 19, 19, 29,  90), BoolValue = true,  GuidValue = new Guid("03021d18-97f0-4dc0-98d0-f0c7df4a1230"), SmallIntValue = 12, StringValue = "0"  }
				});

			if (DataConnection.TraceSwitch.TraceInfo)
				Console.WriteLine("\nBulkCopy Parent\n");

			db.BulkCopy(
				options,
				new[]
				{
					new Parent { ParentID = 1, Value1 = 1    },
					new Parent { ParentID = 2, Value1 = null },
					new Parent { ParentID = 3, Value1 = 3    },
					new Parent { ParentID = 4, Value1 = null },
					new Parent { ParentID = 5, Value1 = 5    },
					new Parent { ParentID = 6, Value1 = 6    },
					new Parent { ParentID = 7, Value1 = 1    }
				});

			if (DataConnection.TraceSwitch.TraceInfo)
				Console.WriteLine("\nBulkCopy Child\n");

			db.BulkCopy(
				options,
				new[]
				{
					new Child { ParentID = 1, ChildID = 11 },
					new Child { ParentID = 2, ChildID = 21 },
					new Child { ParentID = 2, ChildID = 22 },
					new Child { ParentID = 3, ChildID = 31 },
					new Child { ParentID = 3, ChildID = 32 },
					new Child { ParentID = 3, ChildID = 33 },
					new Child { ParentID = 4, ChildID = 41 },
					new Child { ParentID = 4, ChildID = 42 },
					new Child { ParentID = 4, ChildID = 43 },
					new Child { ParentID = 4, ChildID = 44 },
					new Child { ParentID = 6, ChildID = 61 },
					new Child { ParentID = 6, ChildID = 62 },
					new Child { ParentID = 6, ChildID = 63 },
					new Child { ParentID = 6, ChildID = 64 },
					new Child { ParentID = 6, ChildID = 65 },
					new Child { ParentID = 6, ChildID = 66 },
					new Child { ParentID = 7, ChildID = 77 }
				});

			if (DataConnection.TraceSwitch.TraceInfo)
				Console.WriteLine("\nBulkCopy GrandChild\n");

			db.BulkCopy(
				options,
				new[]
				{
					new GrandChild { ParentID = 1, ChildID = 11, GrandChildID = 111 },
					new GrandChild { ParentID = 2, ChildID = 21, GrandChildID = 211 },
					new GrandChild { ParentID = 2, ChildID = 21, GrandChildID = 212 },
					new GrandChild { ParentID = 2, ChildID = 22, GrandChildID = 221 },
					new GrandChild { ParentID = 2, ChildID = 22, GrandChildID = 222 },
					new GrandChild { ParentID = 3, ChildID = 31, GrandChildID = 311 },
					new GrandChild { ParentID = 3, ChildID = 31, GrandChildID = 312 },
					new GrandChild { ParentID = 3, ChildID = 31, GrandChildID = 313 },
					new GrandChild { ParentID = 3, ChildID = 32, GrandChildID = 321 },
					new GrandChild { ParentID = 3, ChildID = 32, GrandChildID = 322 },
					new GrandChild { ParentID = 3, ChildID = 32, GrandChildID = 323 },
					new GrandChild { ParentID = 3, ChildID = 33, GrandChildID = 331 },
					new GrandChild { ParentID = 3, ChildID = 33, GrandChildID = 332 },
					new GrandChild { ParentID = 3, ChildID = 33, GrandChildID = 333 },
					new GrandChild { ParentID = 4, ChildID = 41, GrandChildID = 411 },
					new GrandChild { ParentID = 4, ChildID = 41, GrandChildID = 412 },
					new GrandChild { ParentID = 4, ChildID = 41, GrandChildID = 413 },
					new GrandChild { ParentID = 4, ChildID = 41, GrandChildID = 414 },
					new GrandChild { ParentID = 4, ChildID = 42, GrandChildID = 421 },
					new GrandChild { ParentID = 4, ChildID = 42, GrandChildID = 422 },
					new GrandChild { ParentID = 4, ChildID = 42, GrandChildID = 423 },
					new GrandChild { ParentID = 4, ChildID = 42, GrandChildID = 424 }
				});


			db.BulkCopy(
				options,
				new[]
				{
					new InheritanceParent2 {InheritanceParentId = 1, TypeDiscriminator = null, Name = null },
					new InheritanceParent2 {InheritanceParentId = 2, TypeDiscriminator = 1,    Name = null },
					new InheritanceParent2 {InheritanceParentId = 3, TypeDiscriminator = 2,    Name = "InheritanceParent2" }
				});

			db.BulkCopy(
				options,
				new[]
				{
					new InheritanceChild2() {InheritanceChildId = 1, TypeDiscriminator = null, InheritanceParentId = 1, Name = null },
					new InheritanceChild2() {InheritanceChildId = 2, TypeDiscriminator = 1,    InheritanceParentId = 2, Name = null },
					new InheritanceChild2() {InheritanceChildId = 3, TypeDiscriminator = 2,    InheritanceParentId = 3, Name = "InheritanceParent2" }
				});

			action?.Invoke(db.Connection);
		}
	}

	[Test, Order(0)]
	public void CreateDatabase([CreateDatabaseSources] string context)
	{
		if (TestProvName.IsiSeries(context))
		{
			var script = context.Contains("GAS") ? "DB2iSeriesGAS" : "DB2iSeries";
			RunScript(context, "\nGO\n", script);
		}
	}
}
