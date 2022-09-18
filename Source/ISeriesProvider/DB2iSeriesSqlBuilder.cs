using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Common;
	using LinqToDB.Data;
	using Mapping;
	using SqlProvider;
	using SqlQuery;
	using System.Data.Common;
	using System.Data.SqlTypes;
	using System.Globalization;

	public partial class DB2iSeriesSqlBuilder : BasicSqlBuilder
	{
		public static DB2iSeriesIdentifierQuoteMode IdentifierQuoteMode = DB2iSeriesIdentifierQuoteMode.None;
		
		protected DB2iSeriesDataProvider Provider { get; set; }
		
		public DB2iSeriesSqlProviderFlags DB2iSeriesSqlProviderFlags { get; }

		protected override bool SupportsNullInColumn => false;

		public DB2iSeriesSqlBuilder(
			DB2iSeriesDataProvider provider,
			MappingSchema mappingSchema,
			ISqlOptimizer sqlOptimizer,
			SqlProviderFlags sqlProviderFlags,
			DB2iSeriesSqlProviderFlags db2iSeriesSqlProviderFlags)
			: base(provider, mappingSchema, sqlOptimizer, sqlProviderFlags)
		{
			Provider = provider;
			DB2iSeriesSqlProviderFlags = db2iSeriesSqlProviderFlags;
		}

		DB2iSeriesSqlBuilder(BasicSqlBuilder parentBuilder) 
			: base(parentBuilder)
		{
			if (parentBuilder is DB2iSeriesSqlBuilder dB2ISeriesSqlBuilder)
			{
				Provider = dB2ISeriesSqlBuilder.Provider;
				DB2iSeriesSqlProviderFlags = dB2ISeriesSqlBuilder.DB2iSeriesSqlProviderFlags;
			}
			else
			{
				Provider = DataProvider as DB2iSeriesDataProvider;
			}
		}

		protected override ISqlBuilder CreateSqlBuilder()
		{
			return new DB2iSeriesSqlBuilder(this);
		}

		#region Same as DB2 provider

		protected override void BuildCreateTableIdentityAttribute1(SqlField field)
		{
			StringBuilder.Append("GENERATED ALWAYS AS IDENTITY");
		}

		protected override void BuildEmptyInsert(SqlInsertClause insertClause)
		{
			StringBuilder.Append("VALUES");

			foreach (var _ in insertClause.Into.Fields)
				StringBuilder.Append("(DEFAULT)");

			StringBuilder.AppendLine();
		}

		protected override void BuildInsertOrUpdateQuery(SqlInsertOrUpdateStatement insertOrUpdate)
		{
			if (DB2iSeriesSqlProviderFlags.SupportsMergeStatement)
				BuildInsertOrUpdateQueryAsMerge(insertOrUpdate, $"FROM {Constants.SQL.DummyTableName()} FETCH FIRST 1 ROW ONLY");
			else
				base.BuildInsertOrUpdateQuery(insertOrUpdate);
		}
		
		public override StringBuilder Convert(StringBuilder sb, string value, ConvertType convertType)
		{
			switch (convertType)
			{
				case ConvertType.NameToQueryParameter:
					return DB2iSeriesSqlProviderFlags.SupportsNamedParameters
						? sb.Append(NamedQueryParameterMarkerPrefix).Append(value) : sb.Append(UnnamedParameterMarker);

				case ConvertType.NameToCommandParameter:
				case ConvertType.NameToSprocParameter:
					return DB2iSeriesSqlProviderFlags.SupportsNamedParameters
						? sb.Append(NamedStoredProcedureParameterMarkerPrefix).Append(value) : sb.Append(UnnamedParameterMarker);
					
				case ConvertType.SprocParameterToName:
					return value.Length > 0 && value[0] == NamedStoredProcedureParameterMarkerPrefix[0]
							? sb.Append(value.Substring(1))
							: sb.Append(value);

				case ConvertType.NameToQueryField:
				case ConvertType.NameToQueryFieldAlias:
				case ConvertType.NameToQueryTable:
				case ConvertType.NameToProcedure:
				case ConvertType.NameToPackage:
				case ConvertType.NameToSchema:
				case ConvertType.NameToDatabase:
				case ConvertType.NameToQueryTableAlias:
					if (IdentifierQuoteMode != DB2iSeriesIdentifierQuoteMode.None)
					{
						if (value.Length > 0 && value[0] == '"')
						{
							return sb.Append(value);
						}
						if (IdentifierQuoteMode == DB2iSeriesIdentifierQuoteMode.Quote ||
							value.StartsWith("_") ||
							value.Any(c => char.IsLower(c) || char.IsWhiteSpace(c)))
							return sb.Append('"').Append(value).Append('"');
					}
					break;
			}

			return base.Convert(sb, value, convertType);
		}

		protected override string GetPhysicalTableName(ISqlTableSource table, string alias, bool ignoreTableExpression = false, string defaultDatabaseName = null)
		{
			var name = base.GetPhysicalTableName(table, alias, ignoreTableExpression, defaultDatabaseName);

			if (table.SqlTableType == SqlTableType.Function)
				return $"TABLE({name})";

			return name;
		}

		public override StringBuilder BuildObjectName(StringBuilder sb, SqlObjectName name, ConvertType objectType, bool escape, TableOptions tableOptions)
		{
			if (objectType == ConvertType.NameToProcedure && name.Database != null)
				throw new LinqToDBException("DB2 for i cannot address functions/procedures with database name specified.");

			var schemaName = name.Schema;
			if (schemaName == null && tableOptions.IsTemporaryOptionSet())
				schemaName = "SESSION";

			// "db..table" syntax not supported
			if (name.Database != null && schemaName == null)
				throw new LinqToDBException("DB2 for i requires schema name if database name provided.");

			if (name.Database != null)
			{
				(escape ? Convert(sb, name.Database, ConvertType.NameToDatabase) : sb.Append(name.Database))
					.Append('.');
				if (schemaName == null)
					sb.Append('.');
			}

			if (schemaName != null)
			{
				(escape ? Convert(sb, schemaName, ConvertType.NameToSchema) : sb.Append(schemaName))
					.Append('.');
			}

			return escape ? Convert(sb, name.Name, objectType) : sb.Append(name.Name);
		}

		protected override void BuildCreateTableCommand(SqlTable table)
		{
			if (table.TableOptions.IsTemporaryOptionSet())
			{
				string command;
				switch (table.TableOptions & TableOptions.IsGlobalTemporaryStructure)
				{
					case TableOptions.IsGlobalTemporaryStructure:
						command = "DECLARE GLOBAL TEMPORARY TABLE ";
						break;
					case var value:
						throw new InvalidOperationException($"Incompatible table options '{value}'");
				}
				StringBuilder.Append(command);
			}
			else
			{
				base.BuildCreateTableCommand(table);
			}
		}

		protected override void BuildCreateTablePrimaryKey(SqlCreateTableStatement createTable, string pkName, IEnumerable<string> fieldNames)
		{
			// DB2 doesn't support constraints on temp tables
			if (createTable.Table.TableOptions.IsTemporaryOptionSet())
			{
				var idx = StringBuilder.Length - 1;
				while (idx >= 0 && StringBuilder[idx] != ',')
					idx--;
				StringBuilder.Length = idx == -1 ? 0 : idx;
				return;
			}

			base.BuildCreateTablePrimaryKey(createTable, pkName, fieldNames);
		}

		#endregion

		#region Similar to DB2 provider

		//Same as DB2 provider except it handles Offset Support
		protected override string LimitFormat(SelectQuery selectQuery)
		{
			return
				DB2iSeriesSqlProviderFlags.SupportsOffsetClause || selectQuery.Select.SkipValue == null
				? "FETCH FIRST {0} ROWS ONLY" : null;
		}

		//Same as DB2 provider - except no identityField internal state is held
		//TODO: Check if idenityField is needed as in DB2 provider
		public override int CommandCount(SqlStatement statement)
		{
			if (statement is SqlTruncateTableStatement trun)
				return !DB2iSeriesSqlProviderFlags.SupportsTruncateTable && trun.ResetIdentity ? 
					1 + trun.Table!.IdentityFields.Count : 1;

			return statement is SqlInsertStatement insertStatement && insertStatement.Insert.WithIdentity ? 2 : 1;
		}

		//Same as DB2 provider except it handles Truncate Support
		protected override void BuildCommand(SqlStatement statement, int commandNumber)
		{
			if (statement is SqlTruncateTableStatement trun)
			{
				if (!DB2iSeriesSqlProviderFlags.SupportsTruncateTable)
				{
					var field = trun.Table!.IdentityFields[commandNumber - 1];

					StringBuilder.Append("ALTER TABLE ");
					BuildObjectName(StringBuilder, trun.Table.TableName, ConvertType.NameToQueryTable, true, trun.Table.TableOptions);
					StringBuilder.Append(" ALTER ");
					Convert(StringBuilder, field.PhysicalName, ConvertType.NameToQueryField);
					StringBuilder.AppendLine(" RESTART WITH 1");
				}
			}
			else
			{
				StringBuilder.AppendLine($"SELECT {Constants.SQL.LastInsertedIdentityGetter} FROM {Constants.SQL.DummyTableName()}");
			}
		}

		//Same as DB2 provider except it handles Truncate Support
		protected override void BuildTruncateTableStatement(SqlTruncateTableStatement truncateTable)
		{
			if (DB2iSeriesSqlProviderFlags.SupportsTruncateTable)
			{
				var table = truncateTable.Table;

				BuildTag(truncateTable);
				AppendIndent();
				StringBuilder.Append("TRUNCATE TABLE ");
				BuildPhysicalTable(table, null);

				if (truncateTable.ResetIdentity)
					StringBuilder.Append(" RESTART IDENTITY");

				StringBuilder.Append(" IMMEDIATE");
			}
			else
				base.BuildTruncateTableStatement(truncateTable);
		}

		//Same as DB2 provider - adds alias handling
		protected override void BuildSelectClause(SelectQuery selectQuery)
		{
			if (selectQuery.HasSetOperators)
			{
				// need to set any column aliases as the same as the top level one
				var topquery = selectQuery;

				while (topquery.ParentSelect != null && topquery.ParentSelect.HasSetOperators)
				{
					topquery = topquery.ParentSelect;
				}
				var aliases = selectQuery.Select.Columns.Select(c => c.Alias).ToArray();

				selectQuery.SetOperators.ForEach((u) =>
				{
					int colNo = 0;
					u.SelectQuery.Select.Columns
					.ForEach(c =>
					{
						c.Alias = aliases[colNo];
						colNo++;
					});
				});
			}

			if (selectQuery.From.Tables.Count == 0)
			{
				AppendIndent().AppendLine("SELECT");
				BuildColumns(selectQuery);
				AppendIndent().AppendLine($"FROM {Constants.SQL.DummyTableName()}");
			}
			else
			{
				if (Provider.ProviderType.IsOleDb())
				{
					AppendIndent();
					StringBuilder.Append("SELECT");

					if (selectQuery.Select.IsDistinct)
						StringBuilder.Append(" DISTINCT");

					BuildSkipFirst(selectQuery);

					//Need to append an extra space
					StringBuilder.Append(' ').AppendLine();
					BuildColumns(selectQuery);
				}
				else
					base.BuildSelectClause(selectQuery);
			}
		}

		#endregion

		#region iDB2 specific

		//OleDb provider needs spaces in specific places
		protected override string Comma => Provider.ProviderType.IsOleDb() ? ", " : base.Comma;
		
		//OleDb provider needs spaces in specific places
		protected override string InlineComma => Provider.ProviderType.IsOleDb() ? ", " : base.InlineComma;
		
		//OleDb provider needs spaces in specific places
		protected override string OpenParens => Provider.ProviderType.IsOleDb() ? "( " : base.OpenParens;

		//Offset clause support
		protected override string OffsetFormat(SelectQuery selectQuery) =>
			DB2iSeriesSqlProviderFlags.SupportsOffsetClause ? "OFFSET {0} ROWS" : null;

		//Offset clause support
		protected override bool OffsetFirst => DB2iSeriesSqlProviderFlags.SupportsOffsetClause;

		//Used for printing parameter information in traces - Decimal handling from DB2 provider
		protected override string GetProviderTypeName(IDataContext dataContext, DbParameter parameter)
		{
			if (parameter.DbType == DbType.Decimal && parameter.Value is decimal decValue)
			{
				var d = new SqlDecimal(decValue);
				return string.Format("({0}{1}{2})", d.Precision.ToString(CultureInfo.InvariantCulture), InlineComma, d.Scale.ToString(CultureInfo.InvariantCulture));
			}

			return Provider switch
			{
				DB2iSeriesDataProvider provider => provider.TryGetProviderParameterName(dataContext, parameter, out var name) ? name : null,
				_ => null
			} ?? base.GetProviderTypeName(dataContext, parameter);
		}

		//Use mapping schema and internal db datatype mapping information to get the appropriate dbType
		protected override void BuildDataTypeFromDataType(SqlDataType type, bool forCreateTable)
		{
			var dbType = MappingSchema.GetDbDataType(type.SystemType, type.Type.DataType, type.Type.Length, type.Type.Precision, type.Type.Scale, false, DB2iSeriesSqlProviderFlags.SupportsNCharTypes);

			StringBuilder.Append(dbType.ToSqlString());
		}

		protected override void BuildDeleteQuery(SqlDeleteStatement deleteStatement)
		{
			if (deleteStatement.With != null)
				throw new NotSupportedException("iSeries doesn't support Cte in Delete statement");

			base.BuildDeleteQuery(deleteStatement);
		}

		protected override void BuildUpdateQuery(SqlStatement statement, SelectQuery selectQuery, SqlUpdateClause updateClause)
		{
			if (statement.GetWithClause() != null)
				throw new NotSupportedException("iSeries doesn't support Cte in Update statement");


			base.BuildUpdateQuery(statement, selectQuery, updateClause);
		}

		//Null value casting
		protected override void BuildColumnExpression(SelectQuery selectQuery, ISqlExpression expr, string alias, ref bool addAlias)
		{
			//Null values need to be explicitly casted
			if (expr is SqlValue value && value.Value == null)
			{
				var colType = MappingSchema.GetDbTypeForCast(this.DB2iSeriesSqlProviderFlags, new SqlDataType(value.ValueType)).ToSqlString();
				expr = new SqlExpression(expr.SystemType, "Cast({0} as {1})", Precedence.Primary, expr, new SqlExpression(colType, Precedence.Primary));
			}

			base.BuildColumnExpression(selectQuery, expr, alias, ref addAlias);
		}

		protected override StringBuilder BuildExpression(ISqlExpression expr, bool buildTableName, bool checkParentheses, string alias, ref bool addAlias, bool throwExceptionIfTableNotFound = true)
		{
			//Parameter markers need to be explicitly type casted in many cases in iDB2
			if (expr is SqlParameter parameter && parameter.Name != null)
			{
				var typeToCast = MappingSchema.GetDbTypeForCast(this.DB2iSeriesSqlProviderFlags, new SqlDataType(parameter.Type));

				//No type found - ommit cast
				if (typeToCast.DataType == DataType.Undefined)
				{
					base.BuildExpression(expr, buildTableName, checkParentheses, alias, ref addAlias, throwExceptionIfTableNotFound);
				}
				//Cast to returned type
				else
				{
					StringBuilder.Append("CAST(");
					base.BuildExpression(expr, buildTableName, checkParentheses, alias, ref addAlias, throwExceptionIfTableNotFound);
					StringBuilder.Append(" AS ");
					StringBuilder.Append(typeToCast.ToSqlString());
					StringBuilder.Append(')');
				}

				return StringBuilder;
			}
			if (expr is SqlValue value && value.Value == null)
			{
				var typeToCast = MappingSchema.GetDbTypeForCast(this.DB2iSeriesSqlProviderFlags, new SqlDataType(value.ValueType));

				//No type found - ommit cast
				if (typeToCast.DataType == DataType.Undefined)
				{
					base.BuildExpression(expr, buildTableName, checkParentheses, alias, ref addAlias, throwExceptionIfTableNotFound);
				}
				//Cast to returned type
				else
				{
					StringBuilder.Append("CAST(NULL AS ");
					StringBuilder.Append(typeToCast.ToSqlString());
					StringBuilder.Append(')');
				}

				return StringBuilder;
			}
			else
				return base.BuildExpression(expr, buildTableName, checkParentheses, alias, ref addAlias, throwExceptionIfTableNotFound);
		}

		//Linq2db calls this method to build and explicit cast around an expression
		protected override void BuildTypedExpression(SqlDataType dataType, ISqlExpression value)
		{
			//Explicitly add a Cast around the expression
			//If the expression is a parameter marker or value try to get the type to cast, 
			//otherwise return variant datatype, to differentiate from undefined
			var typeToCast = value switch
			{
				SqlParameter sqlParameter when sqlParameter.Name != null => MappingSchema.GetDbTypeForCast(this.DB2iSeriesSqlProviderFlags, dataType),
				SqlValue _ => MappingSchema.GetDbTypeForCast(this.DB2iSeriesSqlProviderFlags, dataType),
				_ => new DbDataType(null, DataType.Variant)
			};

			//No special handling
			if (typeToCast.DataType == DataType.Variant)
				base.BuildTypedExpression(dataType, value);
			//Null means no suitable type found, don't cast
			else if (typeToCast.DataType == DataType.Undefined)
				base.BuildExpression(value);
			//Case with type returned
			else
			{
				StringBuilder.Append("CAST(");
				BuildExpression(value);
				StringBuilder.Append(" AS ");
				StringBuilder.Append(typeToCast.ToSqlString());
				StringBuilder.Append(')');
			}

		}

		//Same as BasicBuilder but handles allow NULL as blank
		protected override void BuildCreateTableNullAttribute(SqlField field, DefaultNullable defaulNullable)
		{
			if (defaulNullable == DefaultNullable.Null && field.CanBeNull)
				return;

			if (defaulNullable == DefaultNullable.NotNull && !field.CanBeNull)
				return;

			StringBuilder.Append(field.CanBeNull ? " " : "NOT NULL");
		}

		protected override IEnumerable<SqlColumn> GetSelectedColumns(SelectQuery selectQuery)
		{
			//TODO: Test this scenario with AlternativeGetSelectedColumns
			if (NeedSkip(selectQuery.Select.TakeValue, selectQuery.Select.SkipValue) && !selectQuery.OrderBy.IsEmpty)
				return AlternativeGetSelectedColumns(selectQuery, () => base.GetSelectedColumns(selectQuery));

			return base.GetSelectedColumns(selectQuery);
		}

		//Same as BaseSqlBuilder - except reversed first two steps to comply with DB2i cte syntax
		//TODO: Add a test for this scenario with cte
		protected override void BuildInsertQuery(SqlStatement statement, SqlInsertClause insertClause, bool addAlias)
		{
			BuildStep = Step.Tag; BuildTag(statement);
			BuildStep = Step.InsertClause; BuildInsertClause(statement, insertClause, addAlias);
			BuildStep = Step.WithClause; BuildWithClause(statement.GetWithClause());
			
			if (statement.QueryType == QueryType.Insert && statement.SelectQuery.From.Tables.Count != 0)
			{
				BuildStep = Step.SelectClause; BuildSelectClause(statement.SelectQuery);
				BuildStep = Step.FromClause; BuildFromClause(statement, statement.SelectQuery);
				BuildStep = Step.WhereClause; BuildWhereClause(statement.SelectQuery);
				BuildStep = Step.GroupByClause; BuildGroupByClause(statement.SelectQuery);
				BuildStep = Step.HavingClause; BuildHavingClause(statement.SelectQuery);
				BuildStep = Step.OrderByClause; BuildOrderByClause(statement.SelectQuery);
				BuildStep = Step.OffsetLimit; BuildOffsetLimit(statement.SelectQuery);
				BuildStep = Step.QueryExtensions; BuildQueryExtensions(statement);
			}

			if (insertClause.WithIdentity)
				BuildGetIdentity(insertClause);
			else
			{
				BuildStep = Step.Output;
				BuildOutputSubclause(statement.GetOutputClause());
			}
		}

		//Use IF EXISTS syntax
		protected override void BuildDropTableStatement(SqlDropTableStatement dropTable)
		{
			if (DB2iSeriesSqlProviderFlags.SupportsDropTableIfExists)
				this.BuildDropTableStatementIfExists(dropTable);
			else
				base.BuildDropTableStatement(dropTable);
		}

		protected override StringBuilder BuildSqlComment(StringBuilder sb, SqlComment comment)
		{
			//OleDb provider fails with "Prepared statement S000001 in use" when using inline comments.
			//This seems to affect later calls, it probably breaks the connection and connections are pooled.
			if (!Provider.ProviderType.IsOleDb())
				return base.BuildSqlComment(sb, comment);

			return sb;
		}

		protected override void BuildFunction(SqlFunction func)
		{
			base.BuildFunction(func);
		}

		public string BuildStoredProcedureCall(string procedureName, IEnumerable<DataParameter> parameters)
		{
			var callParameters = string.Join(", " , parameters.Select(p => ConvertInline(p.Name, ConvertType.NameToSprocParameter)));

			return $"CALL {procedureName}({callParameters})";
		}

		#endregion
	}
}
