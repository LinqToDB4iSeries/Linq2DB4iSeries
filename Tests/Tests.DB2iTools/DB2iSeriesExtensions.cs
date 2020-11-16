using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Linq;
using LinqToDB.SqlQuery;
using LinqToDB.SchemaProvider;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

#nullable disable

#if NET472
namespace System.Diagnostics.CodeAnalysis
{
}
#endif

namespace IBM.Data.Informix
{
	public class IfxTimeSpan { }
}

namespace Tests
{
	public static class SchemaProviderBaseExtensions
	{
		public static void SetForeignKeyMemberName(GetSchemaOptions getSchemaOptions, TableSchema tableSchema, ForeignKeySchema foreignKeySchema)
		{
			typeof(SchemaProviderBase).GetMethod(nameof(SetForeignKeyMemberName), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(null, new object[] { getSchemaOptions, tableSchema, foreignKeySchema });
		}
	}
}
