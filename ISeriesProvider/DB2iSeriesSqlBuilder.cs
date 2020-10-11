using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using System.Data;
	using LinqToDB.Common;
	using LinqToDB.Mapping;
	using LinqToDB.SqlProvider;
	using LinqToDB.SqlQuery;

	public partial class DB2iSeriesSqlBuilder : BasicSqlBuilder
	{
		public static DB2iSeriesIdentifierQuoteMode IdentifierQuoteMode = DB2iSeriesIdentifierQuoteMode.None;
		protected readonly bool mapGuidAsString;

		protected readonly IDB2iSeriesDataProvider Provider;

		public DB2iSeriesSqlBuilder(
			IDB2iSeriesDataProvider provider,
			MappingSchema mappingSchema,
			ISqlOptimizer sqlOptimizer,
			SqlProviderFlags sqlProviderFlags)
			: this(mappingSchema, sqlOptimizer, sqlProviderFlags)
		{
			Provider = provider;
		}

		// remote context
		public DB2iSeriesSqlBuilder(
			MappingSchema mappingSchema,
			ISqlOptimizer sqlOptimizer,
			SqlProviderFlags sqlProviderFlags)
			: base(mappingSchema, sqlOptimizer, sqlProviderFlags)
		{
			mapGuidAsString = sqlProviderFlags.CustomFlags.Contains(Constants.ProviderFlags.MapGuidAsString);
		}

		protected override ISqlBuilder CreateSqlBuilder()
		{
			return new DB2iSeriesSqlBuilder(Provider, MappingSchema, SqlOptimizer, SqlProviderFlags);
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

		protected override void BuildFunction(SqlFunction func)
		{
			func = ConvertFunctionParameters(func);
			base.BuildFunction(func);
		}

		protected override void BuildInsertOrUpdateQuery(SqlInsertOrUpdateStatement insertOrUpdate)
		{
			BuildInsertOrUpdateQueryAsMerge(insertOrUpdate, $"FROM {Constants.SQL.DummyTableName()} FETCH FIRST 1 ROW ONLY");
		}

		protected override string LimitFormat(SelectQuery selectQuery)
		{
			return selectQuery.Select.SkipValue == null ? " FETCH FIRST {0} ROWS ONLY" : null;
		}

		protected override void BuildCommand(SqlStatement statement, int commandNumber)
		{
			if (statement is SqlTruncateTableStatement trun)
			{
				var field = trun.Table!.IdentityFields[commandNumber - 1];

				StringBuilder.Append("ALTER TABLE ");
				ConvertTableName(StringBuilder, trun.Table.Server, trun.Table.Database, trun.Table.Schema, trun.Table.PhysicalName!);
				StringBuilder.Append(" ALTER ");
				Convert(StringBuilder, field.PhysicalName, ConvertType.NameToQueryField);
				StringBuilder.AppendLine(" RESTART WITH 1");
			}
			else
			{
				StringBuilder.AppendLine($"SELECT {Constants.SQL.LastInsertedIdentityGetter} FROM {Constants.SQL.DummyTableName()}");
			}
		}

		#endregion

		#region Similar to DB2 provider

		//Same as DB2  except it uses local quoteidentifier
		public override StringBuilder Convert(StringBuilder sb, string value, ConvertType convertType)
		{
			switch (convertType)
			{
				case ConvertType.NameToQueryParameter:
					return Provider.ProviderType == DB2iSeriesAdoProviderType.Odbc
						|| Provider.ProviderType == DB2iSeriesAdoProviderType.OleDb
						? sb.Append("?") : sb.Append("@").Append(value);

				case ConvertType.NameToCommandParameter:
				case ConvertType.NameToSprocParameter:
					return Provider.ProviderType == DB2iSeriesAdoProviderType.Odbc
						|| Provider.ProviderType == DB2iSeriesAdoProviderType.OleDb
						? sb.Append("?") : sb.Append(":").Append(value);
					
				case ConvertType.SprocParameterToName:
					return value.Length > 0 && value[0] == ':'
							? sb.Append(value.Substring(1))
							: sb.Append(value);

				case ConvertType.NameToQueryField:
				case ConvertType.NameToQueryFieldAlias:
				case ConvertType.NameToQueryTable:
				case ConvertType.NameToQueryTableAlias:
					if (IdentifierQuoteMode != DB2iSeriesIdentifierQuoteMode.None)
					{
						if (value.Length > 0 && value[0] == '"')
						{
							return sb.Append(value);
						}
						if (IdentifierQuoteMode == DB2iSeriesIdentifierQuoteMode.Quote ||
							value.StartsWith("_") ||
							value.StartsWith("_") ||
							value.Any(c => char.IsLower(c) || char.IsWhiteSpace(c)))
							return sb.Append('"').Append(value).Append('"');
					}
					break;
			}

			return base.Convert(sb, value, convertType);
		}

		//DB2 has a version check and sets internal identity field state
		public override int CommandCount(SqlStatement statement)
		{
			if (statement is SqlTruncateTableStatement trun)
				return trun.ResetIdentity ? 1 + trun.Table!.IdentityFields.Count : 1;

			return statement is SqlInsertStatement insertStatement && insertStatement.Insert.WithIdentity ? 2 : 1;
		}



		//Same as DB2 - except it handles null value handling
		protected override void BuildColumnExpression(SelectQuery selectQuery, ISqlExpression expr, string alias, ref bool addAlias)
		{
			var wrap = false;

			if (expr.SystemType == typeof(bool))
			{
				if (expr is SqlSearchCondition)
					wrap = true;
				else
					wrap = expr is SqlExpression ex && ex.Expr == "{0}" && ex.Parameters.Length == 1 && ex.Parameters[0] is SqlSearchCondition;
			}

			//Null values need to be explicitly casted
			if (expr is SqlValue value && value.Value == null)
			{
				var colType = MappingSchema.GetDbTypeForCast(new SqlDataType(value.ValueType)).ToSqlString();
				expr = new SqlExpression(expr.SystemType, "Cast({0} as {1})", Precedence.Primary, expr, new SqlExpression(colType, Precedence.Primary));
			}

			if (wrap) StringBuilder.Append("CASE WHEN ");
			base.BuildColumnExpression(selectQuery, expr, alias, ref addAlias);
			if (wrap) StringBuilder.Append(" THEN 1 ELSE 0 END");
		}

		//Same as DB2 - db2 also has special check for decimal
		protected override string GetProviderTypeName(IDbDataParameter parameter)
		{
			return Provider switch 
			{
				IDB2iSeriesDataProvider provider => provider.TryGetProviderParameterName(parameter, MappingSchema, out var name) ? name : null,
				_ => null
			} ?? base.GetProviderTypeName(parameter);
		}

		//Same as DB2 - adds alias handling
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
				base.BuildSelectClause(selectQuery);
			}
		}

		#endregion

		#region iDB2 specific

		//OK
		protected override void BuildDataTypeFromDataType(SqlDataType type, bool forCreateTable)
		{
			var dbType = MappingSchema.GetDbDataType(type.SystemType, type.Type.DataType, type.Type.Length, type.Type.Precision, type.Type.Scale, forCreateTable);

			StringBuilder.Append(dbType.ToSqlString());
		}

		//OK
		protected override void BuildDeleteQuery(SqlDeleteStatement deleteStatement)
		{
			if (deleteStatement.With != null)
				throw new NotSupportedException("iSeries doesn't support Cte in Delete statement");

			base.BuildDeleteQuery(deleteStatement);
		}

		//OK
		protected override void BuildUpdateQuery(SqlStatement statement, SelectQuery selectQuery, SqlUpdateClause updateClause)
		{
			if (statement.GetWithClause() != null)
				throw new NotSupportedException("iSeries doesn't support Cte in Update statement");


			base.BuildUpdateQuery(statement, selectQuery, updateClause);
		}

		//OK
		protected override StringBuilder BuildExpression(ISqlExpression expr, bool buildTableName, bool checkParentheses, string alias, ref bool addAlias, bool throwExceptionIfTableNotFound = true)
		{
			//Parameter markers need to be explicitly casted in iDB2
			if (expr is SqlParameter parameter && parameter.Name != null)
			{
				var typeToCast = MappingSchema.GetDbTypeForCast(new SqlDataType(parameter.Type));

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
					StringBuilder.Append(")");
				}

				return StringBuilder;
			}
			else
				return base.BuildExpression(expr, buildTableName, checkParentheses, alias, ref addAlias, throwExceptionIfTableNotFound);
		}

		//OK
		protected override void BuildTypedExpression(SqlDataType dataType, ISqlExpression value)
		{
			//Explicitly add a Cast around the expression
			//If the expression is a parameter marker or value try to get the type to cast, 
			//otherwise return variant datatype, to differentiate from undefined
			var typeToCast = value switch
			{
				SqlParameter sqlParameter when sqlParameter.Name != null => MappingSchema.GetDbTypeForCast(dataType),
				SqlValue _ => MappingSchema.GetDbTypeForCast(dataType),
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
				StringBuilder.Append(")");
			}

		}

		//OK - Same as Basic but handles NULL as blank
		protected override void BuildCreateTableNullAttribute(SqlField field, DefaultNullable defaulNullable)
		{
			if (defaulNullable == DefaultNullable.Null && field.CanBeNull)
				return;

			if (defaulNullable == DefaultNullable.NotNull && !field.CanBeNull)
				return;

			StringBuilder.Append(field.CanBeNull ? " " : "NOT NULL");
		}

		//TODO: Test this scenario with AlternativeGetSelectedColumns
		protected override IEnumerable<SqlColumn> GetSelectedColumns(SelectQuery selectQuery)
		{
			if (NeedSkip(selectQuery) && !selectQuery.OrderBy.IsEmpty)
				return AlternativeGetSelectedColumns(selectQuery, () => base.GetSelectedColumns(selectQuery));

			return base.GetSelectedColumns(selectQuery);
		}

		//Test - Why nop when statement is update?
		protected override void BuildFromClause(SqlStatement statement, SelectQuery selectQuery)
		{
			if (!statement.IsUpdate())
				base.BuildFromClause(statement, selectQuery);
		}

		//Test code coverage - Try to test scenario
		protected override void BuildPredicate(ISqlPredicate predicate)
		{
			switch (predicate.ElementType)
			{
				case QueryElementType.ExprExprPredicate:

					var ep = (SqlPredicate.ExprExpr)predicate;

					if (ep.Expr1 is SqlFunction function
						&& function.Name == "Date"
						&& ep.Expr2 is SqlParameter parameter)
						parameter.Type = parameter.Type.WithDataType(DataType.Date);

					break;
			}

			base.BuildPredicate(predicate);
		}

		//Same as Base - except reversed first two steps, needs testing
		protected override void BuildInsertQuery(SqlStatement statement, SqlInsertClause insertClause, bool addAlias)
		{
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
			}

			if (insertClause.WithIdentity)
				BuildGetIdentity(insertClause);
		}

		#endregion
	}
}
