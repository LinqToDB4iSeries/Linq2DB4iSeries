using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using LinqToDB.SchemaProvider;
using LinqToDB.Common;
using LinqToDB.Data;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesSchemaProvider : SchemaProviderBase
	{
		private readonly DB2iSeriesDataProvider provider;

		public DB2iSeriesSchemaProvider(DB2iSeriesDataProvider provider)
		{
			this.provider = provider;
		}

		protected override DataType GetDataType(string dataType, string columnType, long? length, int? prec, int? scale)
		{
			return dataType switch
			{
				"BIGINT" => DataType.Int64,
				"BINARY" => DataType.Binary,
				"BLOB" => DataType.Blob,
				"CHAR" => DataType.Char,
				"CHAR FOR BIT DATA" => DataType.Binary,
				"CLOB" => DataType.Text,
				"DATALINK" => DataType.Undefined,
				"DATE" => DataType.Date,
				"DBCLOB" => DataType.NText,
				"DECIMAL" => DataType.Decimal,
				"DOUBLE" => DataType.Double,
				"GRAPHIC" => DataType.NChar,
				"INTEGER" => DataType.Int32,
				"NUMERIC" => DataType.Decimal,
				"REAL" => DataType.Single,
				"ROWID" => DataType.Undefined,
				"SMALLINT" => DataType.Int16,
				"TIME" => DataType.Time,
				"TIMESTAMP" => DataType.Timestamp,
				"VARBINARY" => DataType.VarBinary,
				"VARCHAR" => DataType.VarChar,
				"VARCHAR FOR BIT DATA" => DataType.VarBinary,
				"VARGRAPHIC" => DataType.NVarChar,
				"NCHAR" => DataType.NChar,
				"NVARCHAR" => DataType.NVarChar,
				"NCLOB" => DataType.NText,
				"DECFLOAT" => DataType.Decimal,
				_ => DataType.Undefined,
			};
		}

		protected override List<ColumnInfo> GetColumns(DataConnection dataConnection, GetSchemaOptions options)
		{
			var delimiter = dataConnection.GetDelimiter();
			var sql = $@"
				Select 
				  Column_text 
				, case when CCSID = 65535 and Data_Type in ('CHAR', 'VARCHAR') then Data_Type || ' FOR BIT DATA' else Data_Type end as Data_Type
				, Is_Identity
				, Is_Nullable
				, Length
				, COALESCE(Numeric_Scale , 0) Numeric_Scale
				, Ordinal_Position
				, Column_Name
				, Table_Name
				, Table_Schema
				, Column_Name
				From QSYS2{delimiter}SYSCOLUMNS
				where System_Table_Schema in('{dataConnection.GetLibList()}')
				 ";

			ColumnInfo drf(IDataReader dr)
			{
				var ci = new ColumnInfo
				{
					DataType = dr["Data_Type"].ToString().TrimEnd(),
					Description = dr["Column_Text"].ToString().TrimEnd(),
					IsIdentity = dr["Is_Identity"].ToString().TrimEnd() == "YES",
					IsNullable = dr["Is_Nullable"].ToString().TrimEnd() == "Y",
					Name = dr["Column_Name"].ToString().TrimEnd(),
					Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_Position"]),
					TableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["Table_Schema"]).TrimEnd() + "." + Convert.ToString(dr["Table_Name"]).TrimEnd(),
				};
				SetColumnParameters(ci, Convert.ToInt64(dr["Length"]), Convert.ToInt32(dr["Numeric_Scale"]));
				return ci;
			}

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

		protected override IReadOnlyCollection<ForeignKeyInfo> GetForeignKeys(DataConnection dataConnection, IEnumerable<TableSchema> tables, GetSchemaOptions options)
		{
			var delimiter = dataConnection.GetDelimiter();
			var sql = $@"
			  Select ref.Constraint_Name 
			  , fk.Ordinal_Position
			  , fk.Column_Name  As ThisColumn
			  , fk.Table_Name   As ThisTable
			  , fk.Table_Schema As ThisSchema
			  , uk.Column_Name  As OtherColumn
			  , uk.Table_Schema As OtherSchema
			  , uk.Table_Name   As OtherTable
			  From QSYS2{delimiter}SYSREFCST ref
			  Join QSYS2{delimiter}SYSKEYCST fk on(fk.Constraint_Schema, fk.Constraint_Name) = (ref.Constraint_Schema, ref.Constraint_Name)
			  Join QSYS2{delimiter}SYSKEYCST uk on(uk.Constraint_Schema, uk.Constraint_Name) = (ref.Unique_Constraint_Schema, ref.Unique_Constraint_Name)
			  Where uk.Ordinal_Position = fk.Ordinal_Position
			  And fk.System_Table_Schema in('{dataConnection.GetLibList()}')
			  Order By ThisSchema, ThisTable, Constraint_Name, Ordinal_Position
			  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			ForeignKeyInfo drf(IDataReader dr) => new ForeignKeyInfo
			{
				Name = dr["Constraint_Name"].ToString().TrimEnd(),
				Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_Position"]),
				OtherColumn = dr["OtherColumn"].ToString().TrimEnd(),
				OtherTableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["OtherSchema"]).TrimEnd() + "." + Convert.ToString(dr["OtherTable"]).TrimEnd(),
				ThisColumn = dr["ThisColumn"].ToString().TrimEnd(),
				ThisTableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["ThisSchema"]).TrimEnd() + "." + Convert.ToString(dr["ThisTable"]).TrimEnd()
			};

			return dataConnection.Query(drf, sql).ToList();
		}

		protected override IReadOnlyCollection<PrimaryKeyInfo> GetPrimaryKeys(DataConnection dataConnection, IEnumerable<TableSchema> tables, GetSchemaOptions options)
		{
			var delimiter = dataConnection.GetDelimiter();
			var sql = $@"
			  Select cst.constraint_Name  
				   , cst.table_SCHEMA
				   , cst.table_NAME 
				   , col.Ordinal_position 
				   , col.Column_Name   
			  From QSYS2{delimiter}SYSKEYCST col
			  Join QSYS2{delimiter}SYSCST    cst On(cst.constraint_SCHEMA, cst.constraint_NAME, cst.constraint_type) = (col.constraint_SCHEMA, col.constraint_NAME, 'PRIMARY KEY')
			  And cst.System_Table_Schema in('{dataConnection.GetLibList()}')
			  Order By cst.table_SCHEMA, cst.table_NAME, col.Ordinal_position
			  ";

			PrimaryKeyInfo drf(IDataReader dr) => new PrimaryKeyInfo
			{
				ColumnName = Convert.ToString(dr["Column_Name"]).TrimEnd(),
				Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_position"]),
				PrimaryKeyName = Convert.ToString(dr["constraint_Name"]).TrimEnd(),
				TableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["table_SCHEMA"]).TrimEnd() + "." + Convert.ToString(dr["table_NAME"]).TrimEnd()
			};

			return dataConnection.Query(drf, sql).ToList();
		}

		protected override List<ProcedureInfo> GetProcedures(DataConnection dataConnection, GetSchemaOptions options)
		{
			var sql = $@"
			  Select
				CAST(CURRENT_SERVER AS VARCHAR(128)) AS Catalog_Name
			  , Function_Type
			  , Routine_Definition
			  , Routine_Name
			  , Routine_Schema
			  , Routine_Type
			  , Specific_Name
			  , Specific_Schema
			  From QSYS2{dataConnection.GetDelimiter()}SYSROUTINES 
			  Where Specific_Schema in('{dataConnection.GetLibList()}')
			  Order By Specific_Schema, Specific_Name
			  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			var defaultSchema = dataConnection.Execute<string>("select current_schema from sysibm.sysdummy1");

			ProcedureInfo drf(IDataReader dr)
			{
				return new ProcedureInfo
				{
					CatalogName = Convert.ToString(dr["Catalog_Name"]).TrimEnd(),
					IsDefaultSchema = Convert.ToString(dr["Routine_Schema"]).TrimEnd() == defaultSchema,
					IsFunction = Convert.ToString(dr["Routine_Type"]) == "FUNCTION",
					IsTableFunction = Convert.ToString(dr["Function_Type"]) == "T",
					ProcedureDefinition = Convert.ToString(dr["Routine_Definition"]).TrimEnd(),
					ProcedureID = dataConnection.Connection.Database + "." + Convert.ToString(dr["Specific_Schema"]).TrimEnd() + "." + Convert.ToString(dr["Specific_Name"]).TrimEnd(),
					ProcedureName = Convert.ToString(dr["Routine_Name"]).TrimEnd(),
					SchemaName = Convert.ToString(dr["Routine_Schema"]).TrimEnd()
				};
			}

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

		protected override List<ProcedureParameterInfo> GetProcedureParameters(DataConnection dataConnection, IEnumerable<ProcedureInfo> procedures, GetSchemaOptions options)
		{
			var sql = $@"
			  Select 
				CHARACTER_MAXIMUM_LENGTH
			  , Data_Type
			  , Numeric_Precision
			  , Numeric_Scale
			  , Ordinal_position
			  , Parameter_Mode
			  , Parameter_Name
			  , Specific_Name
			  , Specific_Schema
			  From QSYS2{dataConnection.GetDelimiter()}SYSPARMS 
			  where Specific_Schema in('{dataConnection.GetLibList()}')
			  Order By Specific_Schema, Specific_Name, Parameter_Name
			  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			ProcedureParameterInfo drf(IDataReader dr)
			{
				return new ProcedureParameterInfo
				{
					DataType = Convert.ToString(dr["Parameter_Name"]),
					IsIn = dr["Parameter_Mode"].ToString().Contains("IN"),
					IsOut = dr["Parameter_Mode"].ToString().Contains("OUT"),
					Length = Converter.ChangeTypeTo<long?>(dr["CHARACTER_MAXIMUM_LENGTH"]),
					Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_position"]),
					ParameterName = Convert.ToString(dr["Parameter_Name"]).TrimEnd(),
					Precision = Converter.ChangeTypeTo<int?>(dr["Numeric_Precision"]),
					ProcedureID = dataConnection.Connection.Database + "." + Convert.ToString(dr["Specific_Schema"]).TrimEnd() + "." + Convert.ToString(dr["Specific_Name"]).TrimEnd(),
					Scale = Converter.ChangeTypeTo<int?>(dr["Numeric_Scale"]),
				};
			}
			
			return dataConnection.Query(drf, sql).ToList();
		}

		protected override string GetProviderSpecificTypeNamespace()
		{
			return provider.ProviderType switch
			{
				DB2iSeriesAdoProviderType.AccessClient => DB2iSeriesAccessClientProviderAdapter.AssemblyName,
				DB2iSeriesAdoProviderType.Odbc => OdbcProviderAdapter.AssemblyName,
				DB2iSeriesAdoProviderType.OleDb => OleDbProviderAdapter.AssemblyName,
				DB2iSeriesAdoProviderType.DB2 => DB2.DB2ProviderAdapter.AssemblyName,
				_ => throw ExceptionHelper.InvalidAdoProvider(provider.ProviderType)
			};
		}

		protected override List<DataTypeInfo> GetDataTypes(DataConnection dataConnection)
		{
			if (provider.ProviderType == DB2iSeriesAdoProviderType.Odbc)
			{
				DataTypesSchema = ((DbConnection)dataConnection.Connection).GetSchema("DataTypes");

				return DataTypesSchema.AsEnumerable()
					.Select(t => new DataTypeInfo
					{
						TypeName = t.Field<string>("TypeName"),
						DataType = t.Field<string>("TypeName") == "XML" ? "System.String" : t.Field<string>("DataType"),
						CreateFormat = t.Field<string>("CreateFormat"),
						CreateParameters = t.Field<string>("CreateParameters"),
						ProviderDbType = t.Field<string>("TypeName") == "XML" ? (int)OdbcProviderAdapter.OdbcType.NText : t.Field<int>("ProviderDbType"),
					})
					.ToList();
			}
			else if (provider.ProviderType == DB2iSeriesAdoProviderType.DB2)
			{
				DataTypesSchema = ((DbConnection)dataConnection.Connection).GetSchema("DataTypes");

				return DataTypesSchema.AsEnumerable()
					.Select(t => new DataTypeInfo
					{
						TypeName = t.Field<string>("SQL_TYPE_NAME"),
						DataType = t.Field<string>("FRAMEWORK_TYPE"),
						CreateParameters = t.Field<string>("CREATE_PARAMS"),
					})
					.Union(
					new[]
					{
					new DataTypeInfo { TypeName = "CHARACTER", CreateParameters = "LENGTH", DataType = "System.String" }
					})
					.ToList();
			}
			else
				return base.GetDataTypes(dataConnection);
		}

		protected override List<TableInfo> GetTables(DataConnection dataConnection, GetSchemaOptions options)
		{
			var sql = $@"
				  Select 
					CAST(CURRENT_SERVER AS VARCHAR(128)) AS Catalog_Name
				  , Table_Schema
				  , Table_Name
				  , Table_Text
				  , Table_Type
				  , System_Table_Schema
				  From QSYS2{dataConnection.GetDelimiter()}SYSTABLES 
				  Where Table_Type In('L', 'P', 'T', 'V')
				  And System_Table_Schema in ('{dataConnection.GetLibList()}')	
				  Order By System_Table_Schema, System_Table_Name
				 ";

			var defaultSchema = dataConnection.Execute<string>("select current_schema from sysibm.sysdummy1");
			
			TableInfo drf(IDataReader dr) => new TableInfo
			{
				CatalogName = dr["Catalog_Name"].ToString().TrimEnd(),
				Description = dr["Table_Text"].ToString().TrimEnd(),
				IsDefaultSchema = dr["System_Table_Schema"].ToString().TrimEnd() == defaultSchema,
				IsView = new[] { "L", "V" }.Contains(dr["Table_Type"].ToString()),
				SchemaName = dr["Table_Schema"].ToString().TrimEnd(),
				TableID = dataConnection.Connection.Database + "." + dr["Table_Schema"].ToString().TrimEnd() + "." + dr["Table_Name"].ToString().TrimEnd(),
				TableName = dr["Table_Name"].ToString().TrimEnd()
			};
			
			return dataConnection.Query(drf, sql).ToList();
		}

		#region Helpers

		private static void SetColumnParameters(ColumnInfo ci, long? size, int? scale)
		{
			switch (ci.DataType)
			{
				case "DECIMAL":
				case "NUMERIC":
					if ((size ?? 0) > 0)
						ci.Precision = (int?)size.Value;
					
					if ((scale ?? 0) > 0)
						ci.Scale = scale;
					
					break;
				case "BINARY":
				case "BLOB":
				case "CHAR":
				case "CHAR FOR BIT DATA":
				case "CLOB":
				case "DATALINK":
				case "DBCLOB":
				case "GRAPHIC":
				case "VARBINARY":
				case "VARCHAR":
				case "VARCHAR FOR BIT DATA":
				case "VARGRAPHIC":
					ci.Length = size;
					break;
				case "INTEGER":
				case "SMALLINT":
				case "BIGINT":
				case "TIMESTMP":
				case "DATE":
				case "TIME":
				case "VARG":
				case "DECFLOAT":
				case "FLOAT":
				case "ROWID":
				case "VARBIN":
				case "XML":
					break;
				default:
					throw new NotImplementedException($"unknown data type: {ci.DataType}");
			}
		}

		#endregion
	}
}
