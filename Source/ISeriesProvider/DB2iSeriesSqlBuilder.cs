using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using static LinqToDB.Sql;

using LinqToDB.Data;
using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal partial class DB2iSeriesSqlBuilder : BasicSqlBuilder<DB2iSeriesOptions>
	{
		protected DB2iSeriesDataProvider Provider { get; set; }

		public DB2iSeriesSqlProviderFlags DB2iSeriesSqlProviderFlags { get; }

		public override bool CteFirst => false;

		public DB2iSeriesSqlBuilder(
			DB2iSeriesDataProvider provider,
			MappingSchema mappingSchema,
			DataOptions options,
			ISqlOptimizer sqlOptimizer,
			SqlProviderFlags sqlProviderFlags,
			DB2iSeriesSqlProviderFlags db2iSeriesSqlProviderFlags)
			: base(provider, mappingSchema, options, sqlOptimizer, sqlProviderFlags)
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
				throw new NotSupportedException("DB2iSeriesSqlBuilder cannot be based off another sql builder.");
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

			foreach (var _ in insertClause.Into!.Fields)
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
				case ConvertType.NameToCommandParameter:
				case ConvertType.NameToSprocParameter:
					return DB2iSeriesSqlProviderFlags.SupportsNamedParameters
						? sb.Append(NamedParameterMarkerPrefix).Append(value) : sb.Append(UnnamedParameterMarker);

				case ConvertType.SprocParameterToName:
					return value.Length > 0 && value[0] == NamedParameterMarkerPrefix
							? sb.AppendSubstring(value, 1)
							: sb.Append(value);

				case ConvertType.NameToQueryField:
				case ConvertType.NameToQueryFieldAlias:
				case ConvertType.NameToQueryTable:
				case ConvertType.NameToProcedure:
				case ConvertType.NameToPackage:
				case ConvertType.NameToSchema:
				case ConvertType.NameToDatabase:
				case ConvertType.NameToQueryTableAlias:
					if (ProviderOptions.IdentifierQuoteMode != DB2iSeriesIdentifierQuoteMode.None)
					{
						if (value.Length > 0 && value[0] == '"')
						{
							return sb.Append(value);
						}
						if (ProviderOptions.IdentifierQuoteMode == DB2iSeriesIdentifierQuoteMode.Quote ||
							value.StartsWith("_") ||
							value.Any(c => char.IsLower(c) || char.IsWhiteSpace(c)))
							return sb.Append('"').Append(value).Append('"');
					}
					break;
			}

			return base.Convert(sb, value, convertType);
		}

		protected override string GetPhysicalTableName(ISqlTableSource table, string? alias, bool ignoreTableExpression = false, string? defaultDatabaseName = null, bool withoutSuffix = false)
		{
			var name = base.GetPhysicalTableName(table, alias, ignoreTableExpression, defaultDatabaseName, withoutSuffix: withoutSuffix);

			if (table.SqlTableType == SqlTableType.Function)
				return $"TABLE({name})";

			return name;
		}

		public override StringBuilder BuildObjectName(StringBuilder sb, SqlObjectName name, ConvertType objectType, bool escape, TableOptions tableOptions, bool withoutSuffix = false)
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
				switch (table.TableOptions & TableOptions.IsTemporaryOptionSet)
				{
					case TableOptions.None:
					case TableOptions.NotSet:
						break;
					case var _ when
						table.TableOptions.HasIsTemporary() ||
						table.TableOptions.HasIsLocalTemporaryStructure() ||
						table.TableOptions.HasIsLocalTemporaryData():
						StringBuilder.Append("DECLARE GLOBAL TEMPORARY TABLE ");
						break;
					case var value:
						throw new InvalidOperationException($"Incompatible table options '{value}'");
				}
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
		protected override string? LimitFormat(SelectQuery selectQuery)
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
				StringBuilder.AppendLine(FormattableString.Invariant($"SELECT {Constants.SQL.LastInsertedIdentityGetter} FROM {Constants.SQL.DummyTableName()}"));
			}
		}

		//Same as DB2 provider except it handles Truncate Support
		protected override void BuildTruncateTableStatement(SqlTruncateTableStatement truncateTable)
		{
			if (DB2iSeriesSqlProviderFlags.SupportsTruncateTable)
			{
				var table = truncateTable.Table!;

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
				AppendIndent().AppendLine(FormattableString.Invariant($"FROM {Constants.SQL.DummyTableName()}"));
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
		protected override string? OffsetFormat(SelectQuery selectQuery) =>
			DB2iSeriesSqlProviderFlags.SupportsOffsetClause ? "OFFSET {0} ROWS" : null;

		//Offset clause support
		protected override bool OffsetFirst => DB2iSeriesSqlProviderFlags.SupportsOffsetClause;

		//Use mapping schema and internal db datatype mapping information to get the appropriate dbType
		protected override void BuildDataTypeFromDataType(DbDataType type, bool forCreateTable, bool canBeNull)
		{
			var dbType = MappingSchema.SanitizeDbDataType(type, DB2iSeriesSqlProviderFlags);

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

		protected override void BuildParameter(SqlParameter parameter)
		{
			//Note: DB2 uses a parameter wrap visitor to set NeedsCast instead of casting in all cases,
			//however there are many obscure cases in DB2i where this doesn't work, so we are casting in all cases
			if (BuildStep != Step.TypedExpression)
			{
				//Check if value can be accessed to get the cast from it
				var paramValue = parameter.GetParameterValue(OptimizationContext.EvaluationContext.ParameterValues);
				var dbDataType = paramValue.DbDataType;
				
				var saveStep = BuildStep;
				BuildStep = Step.TypedExpression;

				var typeToCast = MappingSchema.GetDbTypeForCast(dbDataType, paramValue.ProviderValue, DB2iSeriesSqlProviderFlags);

				if (typeToCast.DataType != DataType.Undefined)
				{
					BuildTypedExpression(typeToCast, parameter);
				}
				else
				{
					base.BuildParameter(parameter);
				}

				BuildStep = saveStep;

				return;
			}

			base.BuildParameter(parameter);
		}

		public override void BuildSqlValue(SqlValue value)
		{
			if (value.Value == null)
			{
				var typeToCast = MappingSchema.GetDbTypeForCast(value.ValueType, null, DB2iSeriesSqlProviderFlags);

				if (typeToCast.DataType != DataType.Undefined)
				{
					StringBuilder.Append("CAST(NULL AS ");
					BuildDataType(StringBuilder, typeToCast);
					//StringBuilder.Append(typeToCast.ToSqlString());
					StringBuilder.Append(')');
					return;
				}
			}

			base.BuildSqlValue(value);
		}

		//protected override void BuildTypedExpression(DbDataType dataType, ISqlExpression value)
		//{
		//	//No type found, dont' cast
		//	if (dataType.DataType == DataType.Undefined)
		//		BuildExpression(value);
		//	else
		//	{
		//		//StringBuilder.Append("CAST(");
		//		//BuildExpression(value);
		//		//StringBuilder.Append(" AS ");
		//		//StringBuilder.Append(dataType.ToSqlString());
		//		//StringBuilder.Append(')');
		//		base.BuildTypedExpression(dataType, value);
		//	}
		//}

		//Same as BasicBuilder but handles allow NULL as blank
		protected override void BuildCreateTableNullAttribute(SqlField field, DefaultNullable defaulNullable)
		{
			if (defaulNullable == DefaultNullable.Null && field.CanBeNull)
				return;

			if (defaulNullable == DefaultNullable.NotNull && !field.CanBeNull)
				return;

			StringBuilder.Append(field.CanBeNull ? " " : "NOT NULL");
		}

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

		public string BuildStoredProcedureCall(string procedureName, IEnumerable<DataParameter>? parameters)
		{
			var callParameters = parameters is null ? string.Empty :
				string.Join(InlineComma, parameters.Select(p => ConvertInline(p.Name!, ConvertType.NameToSprocParameter)));

			return $"CALL {procedureName}({callParameters})";
		}

		protected override void BuildPredicate(ISqlPredicate predicate)
		{
			// Transform Exists predicate outside the scope of WHERE clauses
			// It would be more efficient to perform in ConvertVisitor if there is a way to scope to WHERE only
			if (predicate is SqlPredicate.Exists existsPredicate
				&& BuildStep != Step.WhereClause)
			{
				var query = reduceSelector(existsPredicate.SubQuery.CloneQuery());

				// If there there are Set operator (e.g. UNION etc), reduce the selector of each set member 
				// and wrap around a subquery with a single row
				if (query.SetOperators.Count > 0)
				{
					for (var i = 0; i < query.SetOperators.Count; i++)
					{
						query.SetOperators[i] = new SqlSetOperator(reduceSelector(query.SetOperators[i].SelectQuery), query.SetOperators[i].Operation);
					}

					// SqlQuery as Table source failes, falling back to raw SQL
					//var subQuery = query;
					//query = prepareQuery(new SelectQuery());
					//query.From.Table(subQuery);

					StringBuilder.Append("(SELECT 1 FROM (");
					var isNullPredicate = new SqlPredicate.IsNull(query, !existsPredicate.IsNot);
					BuildExpression(isNullPredicate.Precedence, isNullPredicate.Expr1);
					StringBuilder.Append(") FETCH FIRST 1 ROWS ONLY) ");
					StringBuilder.Append(isNullPredicate.IsNot ? " IS NOT NULL" : " IS NULL");
				}
				else
				{
					// Fetch a single row for IS NULL to work
					query.Select.Take(1, null);
					base.BuildPredicate(new SqlPredicate.IsNull(query, !existsPredicate.IsNot));
				}
				
				return;
			}

			base.BuildPredicate(predicate);

			// Reduces selector to a single column
			SelectQuery reduceSelector(SelectQuery query)
			{
				if (query.Select.Columns.Count == 0)
					query.Select.Columns.Add(new SqlColumn(query, new SqlExpression(MappingSchema.GetDbDataType(typeof(string)), "'.'")));
				else if (query.Select.Columns.Count > 1)
					query.Select.Columns.RemoveRange(1, query.Select.Columns.Count - 1);

				return query;
			}
		}

		protected override void BuildUpdateSet(SelectQuery? selectQuery, SqlUpdateClause updateClause)
		{
			if (HasHint(updateClause.Table?.SqlQueryExtensions, QueryExtensionScope.TableHint, DB2iSeriesHints.Table.OverridingSystemValue))
				StringBuilder.Append("OVERRIDING SYSTEM VALUE ");

			base.BuildUpdateSet(selectQuery, updateClause);
		}

		protected override void BuildInsertValuesOverrideClause(SqlStatement statement, SqlInsertClause insertClause)
		{
			if (HasHint(insertClause.Into?.SqlQueryExtensions, QueryExtensionScope.TableHint, DB2iSeriesHints.Table.OverridingSystemValue))
				StringBuilder.Append("OVERRIDING SYSTEM VALUE ");

			base.BuildInsertValuesOverrideClause(statement, insertClause);
		}

		private static bool HasHint(IEnumerable<SqlQueryExtension>? sqlQueryExtensions, QueryExtensionScope scope, string hintName)
		{
			return sqlQueryExtensions?
				.FirstOrDefault(x => x.Scope == scope
					&& x.Arguments.TryGetValue("hint", out var hint)
					&& hint is SqlValue hintValue
					&& hintValue.Value?.ToString() == hintName) is not null;
		}

		#endregion
	}
}
