using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using LinqToDB.Data;
using LinqToDB.Internal.DataProvider;
using LinqToDB.Internal.DataProvider.DB2;
using LinqToDB.Internal.SchemaProvider;
using LinqToDB.SchemaProvider;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal class DB2iSeriesSchemaProvider : SchemaProviderBase
	{
		private readonly DB2iSeriesDataProvider provider;

		public DB2iSeriesSchemaProvider(DB2iSeriesDataProvider provider)
		{
			this.provider = provider;
		}

		protected override DataType GetDataType(string? dataType, string? columnType, int? length, int? prec, int? scale)
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
				where Table_Schema in({dataConnection.GetQuotedLibList()})
				 ";

			ColumnInfo drf(DbDataReader dr)
			{
				var ci = new ColumnInfo
				{
					DataType = dr.GetTrimmedString("Data_Type"),
					Description = dr.GetTrimmedString("Column_Text"),
					IsIdentity = dr.GetTrimmedString("Is_Identity") == "YES",
					IsNullable = dr.GetTrimmedString("Is_Nullable") == "Y",
					Name = dr.GetTrimmedString("Column_Name")!,	
					Ordinal = dr.GetInt32("Ordinal_Position"),
					TableID = dataConnection.GetDbConnection().Database + "." + dr.GetTrimmedString("Table_Schema") + "." + dr.GetTrimmedString("Table_Name"),
				};
				SetColumnParameters(ci, dr.GetInt32("Length"), dr.GetInt32("Numeric_Scale"));
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
			  And fk.Table_Schema in({dataConnection.GetQuotedLibList()})
			  Order By ThisSchema, ThisTable, Constraint_Name, Ordinal_Position
			  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			ForeignKeyInfo drf(DbDataReader dr) => new ForeignKeyInfo
			{
				Name = dr.GetTrimmedString("Constraint_Name")!,
				Ordinal = dr.GetInt32("Ordinal_Position"),
				OtherColumn = dr.GetTrimmedString("OtherColumn")!,
				OtherTableID = dataConnection.GetDbConnection().Database + "." + dr.GetTrimmedString("OtherSchema") + "." + dr.GetTrimmedString("OtherTable"),
				ThisColumn = dr.GetTrimmedString("ThisColumn")!,
				ThisTableID = dataConnection.GetDbConnection().Database + "." + dr.GetTrimmedString("ThisSchema") + "." + dr.GetTrimmedString("ThisTable")
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
			  And cst.Table_Schema in({dataConnection.GetQuotedLibList()})
			  Order By cst.table_SCHEMA, cst.table_NAME, col.Ordinal_position
			  ";

			PrimaryKeyInfo drf(DbDataReader dr) => new PrimaryKeyInfo
			{
				ColumnName = dr.GetTrimmedString("Column_Name")!,
				Ordinal = dr.GetInt32("Ordinal_position"),
				PrimaryKeyName = dr.GetTrimmedString("constraint_Name"),
				TableID = dataConnection.GetDbConnection().Database + "." + dr.GetTrimmedString("table_SCHEMA") + "." + dr.GetTrimmedString("table_NAME")
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
			  Where Specific_Schema in({dataConnection.GetQuotedLibList()})
			  Order By Specific_Schema, Specific_Name
			  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			var defaultSchema = dataConnection.Execute<string>("select current_schema from sysibm.sysdummy1");

			ProcedureInfo drf(DbDataReader dr)
			{
				return new ProcedureInfo
				{
					CatalogName = dr.GetTrimmedString("Catalog_Name"),
					IsDefaultSchema = dr.GetTrimmedString("Routine_Schema") == defaultSchema,
					IsFunction = dr.GetString("Routine_Type") == "FUNCTION",
					IsTableFunction = dr.GetString("Function_Type") == "T",
					ProcedureDefinition = dr.GetTrimmedString("Routine_Definition"),
					ProcedureID = dataConnection.GetDbConnection().Database + "." + dr.GetTrimmedString("Specific_Schema") + "." + dr.GetTrimmedString("Specific_Name"),
					ProcedureName = dr.GetTrimmedString("Routine_Name")!,
					SchemaName = dr.GetTrimmedString("Routine_Schema")
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
			  where Specific_Schema in({dataConnection.GetQuotedLibList()})
			  Order By Specific_Schema, Specific_Name, Parameter_Name
			  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			ProcedureParameterInfo drf(DbDataReader dr)
			{
				return new ProcedureParameterInfo
				{
					DataType = dr.GetString("DATA_TYPE"),
					IsIn = dr.GetString("Parameter_Mode")!.Contains("IN"),
					IsOut = dr.GetString("Parameter_Mode")!.Contains("OUT"),
					Length = dr.GetNullableInt32("CHARACTER_MAXIMUM_LENGTH"),
					Ordinal = dr.GetInt32("Ordinal_position"),
					ParameterName = dr.GetTrimmedString("Parameter_Name"),
					Precision = dr.GetNullableInt32("Numeric_Precision"),
					ProcedureID = dataConnection.GetDbConnection().Database + "." + dr.GetTrimmedString("Specific_Schema") + "." + dr.GetTrimmedString("Specific_Name"),
					Scale = dr.GetNullableInt32("Numeric_Scale"),
				};
			}
			
			return dataConnection.Query(drf, sql).ToList();
		}

		protected override string GetProviderSpecificTypeNamespace()
		{
			return provider.ProviderType switch
			{
#if NETFRAMEWORK
				DB2iSeriesProviderType.AccessClient => DB2iSeriesAccessClientProviderAdapter.AssemblyName,
#endif
				DB2iSeriesProviderType.Odbc => OdbcProviderAdapter.AssemblyName,
				DB2iSeriesProviderType.OleDb => OleDbProviderAdapter.AssemblyName,
				DB2iSeriesProviderType.DB2 => DB2ProviderAdapter.AssemblyName,
				_ => throw ExceptionHelper.InvalidAdoProvider(provider.ProviderType)
			};
		}

		protected override List<DataTypeInfo> GetDataTypes(DataConnection dataConnection)
		{
			if (provider.ProviderType.IsOdbc())
			{
				DataTypesSchema = dataConnection.GetDbConnection().GetSchema("DataTypes");

				return DataTypesSchema.AsEnumerable()
					.Select(t => new DataTypeInfo
					{
						TypeName = t.Field<string>("TypeName")!,
						DataType = t.Field<string>("TypeName") == "XML" ? "System.String" : t.Field<string>("DataType")!,
						CreateFormat = t.Field<string>("CreateFormat"),
						CreateParameters = t.Field<string>("CreateParameters"),
						ProviderDbType = t.Field<string>("TypeName") == "XML" ? (int)OdbcProviderAdapter.OdbcType.NText : t.Field<int>("ProviderDbType"),
					})
					.ToList();
			}
			else if (provider.ProviderType.IsDB2())
			{
				DataTypesSchema = dataConnection.GetDbConnection().GetSchema("DataTypes");

				return DataTypesSchema.AsEnumerable()
					.Select(t => new DataTypeInfo
					{
						TypeName = t.Field<string>("SQL_TYPE_NAME")!,
						DataType = t.Field<string>("FRAMEWORK_TYPE")!,
						CreateParameters = t.Field<string>("CREATE_PARAMS"),
					})
					.Union(
					new[]
					{
					new DataTypeInfo { TypeName = "CHARACTER", CreateParameters = "LENGTH", DataType = "System.String", ProviderDbType = 12 }
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
				  And Table_Schema in ({dataConnection.GetQuotedLibList()})	
				  Order By System_Table_Schema, System_Table_Name
				 ";

			var defaultSchema = dataConnection.Execute<string>("select current_schema from sysibm.sysdummy1");
			
			TableInfo drf(DbDataReader dr) => new TableInfo
			{
				CatalogName = dr.GetTrimmedString("Catalog_Name"),
				Description = dr.GetTrimmedString("Table_Text"),
				IsDefaultSchema = dr.GetTrimmedString("Table_Schema") == defaultSchema,
				IsView = new[] { "L", "V" }.Contains(dr.GetTrimmedString("Table_Type") ?? ""),
				SchemaName = dr.GetTrimmedString("Table_Schema"),
				TableID = dataConnection.GetDbConnection().Database + "." + dr.GetTrimmedString("Table_Schema") + "." + dr.GetTrimmedString("Table_Name"),
				TableName = dr.GetTrimmedString("Table_Name")!
			};
			
			return dataConnection.Query(drf, sql).ToList();
		}

		protected override bool GetProcedureSchemaExecutesProcedure => provider.ProviderType.IsAccessClient();

		protected override DataTable? GetProcedureSchema(DataConnection dataConnection, string commandText, CommandType commandType, DataParameter[] parameters, GetSchemaOptions options)
		{
			if (provider.ProviderType.IsOdbc())
			{
				return dataConnection.GetDbConnection().GetSchema("ProcedureColumns", new[] { null, null, commandText });
			}

			if (provider.ProviderType.IsAccessClient())
			{
				throw new LinqToDBException($"{provider.Name} cannot load procedure schema. DB2i provider type {provider.ProviderType} will execute the procedure if schema is requested.");
			}

			return base.GetProcedureSchema(dataConnection, commandText, commandType, parameters, options);
		}

		#region Helpers

		private static void SetColumnParameters(ColumnInfo ci, int? size, int? scale)
		{
			switch (ci.DataType)
			{
				case "DECIMAL":
				case "NUMERIC":
					if ((size ?? 0) > 0)
						ci.Precision = size!.Value;
					
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
