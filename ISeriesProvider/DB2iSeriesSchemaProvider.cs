using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using LinqToDB.Extensions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Common;
	using Data;
	using SchemaProvider;
	
	public class DB2iSeriesSchemaProvider : SchemaProviderBase
	{
		protected override List<ColumnInfo> GetColumns(DataConnection dataConnection)
		{
			var sql = $@"
				Select 
				  Column_text 
				, case when CCSID = 65535 and Data_Type in ('CHAR', 'VARCHAR') then Data_Type || ' FOR BIT DATA' else Data_Type end as Data_Type
				, Is_Identity
				, Is_Nullable
				, Length
				, COALESCE(Numeric_Scale,0) Numeric_Scale
				, Ordinal_Position
				, Column_Name
				, Table_Name
				, Table_Schema
				, Column_Name
				From QSYS2/SYSCOLUMNS
				where System_Table_Schema in('{GetLibList(dataConnection)}')
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

		protected override DataType GetDataType(string dataType, string columnType, long? length, int? prec, int? scale)
		{
			switch (dataType)
			{
				case "BIGINT": return DataType.Int64;
				case "BINARY": return DataType.Binary;
				case "BLOB": return DataType.Blob;
				case "CHAR": return DataType.Char;
				case "CHAR FOR BIT DATA": return DataType.Binary;
				case "CLOB": return DataType.Text;
				case "DATALINK": return DataType.Undefined;
				case "DATE": return DataType.Date;
				case "DBCLOB": return DataType.Undefined;
				case "DECIMAL": return DataType.Decimal;
				case "DOUBLE": return DataType.Double;
				case "GRAPHIC": return DataType.Text;
				case "INTEGER": return DataType.Int32;
				case "NUMERIC": return DataType.Decimal;
				case "REAL": return DataType.Single;
				case "ROWID": return DataType.Undefined;
				case "SMALLINT": return DataType.Int16;
				case "TIME": return DataType.Time;
				case "TIMESTAMP": return DataType.Timestamp;
				case "VARBINARY": return DataType.VarBinary;
				case "VARCHAR": return DataType.VarChar;
				case "VARCHAR FOR BIT DATA": return DataType.VarBinary;
				case "VARGRAPHIC": return DataType.Text;
				default: return DataType.Undefined;
			}
		}

		protected override List<ForeignKeyInfo> GetForeignKeys(DataConnection dataConnection)
		{
			var sql = $@"
		  Select ref.Constraint_Name 
		  , fk.Ordinal_Position
		  , fk.Column_Name  As ThisColumn
		  , fk.Table_Name   As ThisTable
		  , fk.Table_Schema As ThisSchema
		  , uk.Column_Name  As OtherColumn
		  , uk.Table_Schema As OtherSchema
		  , uk.Table_Name   As OtherTable
		  From QSYS2/SYSREFCST ref
		  Join QSYS2/SYSKEYCST fk on(fk.Constraint_Schema, fk.Constraint_Name) = (ref.Constraint_Schema, ref.Constraint_Name)
		  Join QSYS2/SYSKEYCST uk on(uk.Constraint_Schema, uk.Constraint_Name) = (ref.Unique_Constraint_Schema, ref.Unique_Constraint_Name)
		  Where uk.Ordinal_Position = fk.Ordinal_Position
		  And fk.System_Table_Schema in('{GetLibList(dataConnection)}')
		  Order By ThisSchema, ThisTable, Constraint_Name, Ordinal_Position
		  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			Func<IDataReader, ForeignKeyInfo> drf = (IDataReader dr) => new ForeignKeyInfo
			{
				Name = dr["Constraint_Name"].ToString().TrimEnd(),
				Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_Position"]),
				OtherColumn = dr["OtherColumn"].ToString().TrimEnd(),
				OtherTableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["OtherSchema"]).TrimEnd() + "." + Convert.ToString(dr["OtherTable"]).TrimEnd(),
				ThisColumn = dr["ThisColumn"].ToString().TrimEnd(),
				ThisTableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["ThisSchema"]).TrimEnd() + "." + Convert.ToString(dr["ThisTable"]).TrimEnd()
			};

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

		protected override List<PrimaryKeyInfo> GetPrimaryKeys(DataConnection dataConnection)
		{
			var sql = $@"
		  Select cst.constraint_Name  
			   , cst.table_SCHEMA
			   , cst.table_NAME 
			   , col.Ordinal_position 
			   , col.Column_Name   
		  From QSYS2/SYSKEYCST col
		  Join QSYS2/SYSCST    cst On(cst.constraint_SCHEMA, cst.constraint_NAME, cst.constraint_type) = (col.constraint_SCHEMA, col.constraint_NAME, 'PRIMARY KEY')
		  And cst.System_Table_Schema in('{GetLibList(dataConnection)}')
		  Order By cst.table_SCHEMA, cst.table_NAME, col.Ordinal_position
		  ";

			PrimaryKeyInfo drf(IDataReader dr) => new PrimaryKeyInfo
			{
				ColumnName = Convert.ToString(dr["Column_Name"]).TrimEnd(),
				Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_position"]),
				PrimaryKeyName = Convert.ToString(dr["constraint_Name"]).TrimEnd(),
				TableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["table_SCHEMA"]).TrimEnd() + "." + Convert.ToString(dr["table_NAME"]).TrimEnd()
			};

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

		protected override List<ProcedureInfo> GetProcedures(DataConnection dataConnection)
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
		  From QSYS2/SYSROUTINES 
		  Where Specific_Schema in('{GetLibList(dataConnection)}')
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

		protected override List<ProcedureParameterInfo> GetProcedureParameters(DataConnection dataConnection)
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
		  From QSYS2/SYSPARMS 
		  where Specific_Schema in('{GetLibList(dataConnection)}')
		  Order By Specific_Schema, Specific_Name, Parameter_Name
		  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			Func<IDataReader, ProcedureParameterInfo> drf = (IDataReader dr) =>
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
			};
			List<ProcedureParameterInfo> _list = dataConnection.Query(drf, sql).ToList();
			return _list;
		}

		protected override string GetProviderSpecificTypeNamespace()
		{
			return DB2iSeriesTools.AssemblyName;
		}

		protected override List<TableInfo> GetTables(DataConnection dataConnection)
		{
			var sql = $@"
				  Select 
					CAST(CURRENT_SERVER AS VARCHAR(128)) AS Catalog_Name
				  , Table_Schema
				  , Table_Name
				  , Table_Text
				  , Table_Type
				  , System_Table_Schema
				  From QSYS2/SYSTABLES 
				  Where Table_Type In('L', 'P', 'T', 'V')
				  And System_Table_Schema in ('{GetLibList(dataConnection)}')	
				  Order By System_Table_Schema, System_Table_Name
				 ";

			var defaultSchema = dataConnection.Execute<string>("select current_schema from sysibm.sysdummy1");
			Func<IDataReader, TableInfo> drf = (IDataReader dr) => new TableInfo
			{
			    CatalogName = dr["Catalog_Name"].ToString().TrimEnd(),
			    Description = dr["Table_Text"].ToString().TrimEnd(),
			    IsDefaultSchema = dr["System_Table_Schema"].ToString().TrimEnd() == defaultSchema,
			    IsView = new[] { "L", "V" }.Contains<string>(dr["Table_Type"].ToString()),
			    SchemaName = dr["Table_Schema"].ToString().TrimEnd(),
			    TableID = dataConnection.Connection.Database + "." + dr["Table_Schema"].ToString().TrimEnd() + "." + dr["Table_Name"].ToString().TrimEnd(),
			    TableName = dr["Table_Name"].ToString().TrimEnd()
			};
			var _list = dataConnection.Query(drf, sql).ToList();
			return _list;
		}

		#region Helpers

		public static void SetColumnParameters(ColumnInfo ci, long? size, int? scale)
		{
			switch (ci.DataType)
			{
				case "DECIMAL":
				case "NUMERIC":
					if (((size ?? 0)) > 0)
					{
						ci.Precision = (int?)size.Value;
					}
					if (((scale ?? 0)) > 0)
					{
						ci.Scale = scale;
					}
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

	    private string GetLibList(DataConnection dataConnection)
	    {
	        if (dataConnection.Connection == null || dataConnection.Connection.GetType().Name != "iDB2Connection")
	            throw new LinqToDBException("dataconnection is not iDB2Connection.");

	        var libListProp = dataConnection.Connection.GetType()
	            .GetPropertiesEx(BindingFlags.Public | BindingFlags.Instance)
	            .FirstOrDefault(p => p.Name == "LibraryList"); 

	        if (libListProp == null)
	            throw new LinqToDBException("iDB2Connection is missing LibraryList property, perhaps the IBM library has moved to non supported version");

	        var liblist = Expression.Lambda<Func<object>>(
	                Expression.Convert(
	                    Expression.MakeMemberAccess(Expression.Constant(dataConnection.Connection), libListProp),
	                    typeof(object)))
	            .Compile()();

            return string.Join("','", liblist.ToString().Split(','));
	    }
    }
}