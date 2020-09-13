using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using System.Data;
	using LinqToDB.Mapping;
	using LinqToDB.SqlProvider;
	using LinqToDB.SqlQuery;

	public partial class DB2iSeriesSqlBuilder : BasicSqlBuilder
	{
		public static DB2iSeriesIdentifierQuoteMode IdentifierQuoteMode = DB2iSeriesIdentifierQuoteMode.None;
		protected readonly bool mapGuidAsString;

		protected readonly DB2iSeriesDataProvider Provider;

		public DB2iSeriesSqlBuilder(
			DB2iSeriesDataProvider provider,
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
			mapGuidAsString = sqlProviderFlags.CustomFlags.Contains(DB2iSeriesTools.MapGuidAsString);
		}

		#region Same as DB2 provider

		protected override void BuildCreateTableIdentityAttribute1(SqlField field)
		{
			StringBuilder.Append("GENERATED ALWAYS AS IDENTITY");
		}

		protected override void BuildEmptyInsert(SqlInsertClause insertClause)
		{
			StringBuilder.Append("VALUES");

			foreach (var col in insertClause.Into.Fields)
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
			BuildInsertOrUpdateQueryAsMerge(insertOrUpdate, $"FROM {DB2iSeriesTools.iSeriesDummyTableName()} FETCH FIRST 1 ROW ONLY");
		}

		protected override string LimitFormat(SelectQuery selectQuery)
		{
			return selectQuery.Select.SkipValue == null ? " FETCH FIRST {0} ROWS ONLY" : null;
		}

		//iDB2 added VarBinary
		protected override void BuildDataTypeFromDataType(SqlDataType type, bool forCreateTable)
		{
			switch (type.Type.DataType)
			{
				case DataType.DateTime:
				case DataType.DateTime2:
					StringBuilder.Append("timestamp");
					if (type.Type.Precision != null && type.Type.Precision != 6)
						StringBuilder.Append($"({type.Type.Precision})");
					return;
				
				case DataType.Boolean: StringBuilder.Append("smallint"); return;
				case DataType.Guid: StringBuilder.Append("char(16) for bit data"); return;
				case DataType.NVarChar:
					if (type.Type.Length == null || type.Type.Length > 8168 || type.Type.Length < 1)
					{
						StringBuilder
							.Append(type.Type.DataType)
							.Append("(8168)");
						return;
					}

					break;
				//iDB2
				case DataType.UInt64:
					StringBuilder.Append("DECIMAL(28,0)"); return;
				case DataType.Byte:
					StringBuilder.Append("smallint"); return;
				case DataType.VarBinary:
					if (type.Type.Length == null || type.Type.Length > 32704 || type.Type.Length < 1)
					{
						StringBuilder
							.Append(type.Type.DataType)
							.Append("(32704)");
						return;
					}

					break;
			}

			base.BuildDataTypeFromDataType(type, forCreateTable);
		}

		#endregion

		#region Different from DB2 provider

		//DB2 adds identity field handling
		public override int CommandCount(SqlStatement statement)
		{
			return statement is SqlInsertStatement insertStatement && insertStatement.Insert.WithIdentity ? 2 : 1;
		}

		//DB2 adds truncate table handling
		protected override void BuildCommand(SqlStatement selectQuery, int commandNumber) =>
			StringBuilder.AppendLine($"SELECT {DB2iSeriesTools.IdentityColumnSql} FROM {DB2iSeriesTools.iSeriesDummyTableName()}");




		#endregion

		#region iDB2 specific

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
		protected override IEnumerable<SqlColumn> GetSelectedColumns(SelectQuery selectQuery)
		{
			if (NeedSkip(selectQuery) && !selectQuery.OrderBy.IsEmpty)
				return AlternativeGetSelectedColumns(selectQuery, () => base.GetSelectedColumns(selectQuery));

			return base.GetSelectedColumns(selectQuery);
		}

		//OK
		protected override ISqlBuilder CreateSqlBuilder()
		{
			return new DB2iSeriesSqlBuilder(Provider, MappingSchema, SqlOptimizer, SqlProviderFlags);
		}

		public string GetiSeriesType(SqlDataType dataType)
		{
			static string appendLength(SqlDataType type, string typeName, int max)
			{
				var length = (type.Type.Length == null || type.Type.Length > max || type.Type.Length < 1) ? max : type.Type.Length.Value;
				return typeName + $"({length})";
			}
			
			switch (dataType.Type.DataType)
			{
				case DataType.Variant:
				case DataType.Binary:
					return appendLength(dataType, "BINARY", 255);
					// dataType.Type.Length == null ? "BINARY" : $"BINARY({(dataType.Type.Length == 0 ? 1 : dataType.Type.Length)})";
				case DataType.Int64:
				case DataType.UInt32:
					return "BIGINT";
				case DataType.Blob:
					return appendLength(dataType, "BLOB", 2147483647);
					//dataType.Type.Length == null ? "BLOB" : $"BLOB({(dataType.Type.Length == 0 ? 1 : dataType.Type.Length)})";
				case DataType.VarBinary:
					return appendLength(dataType, "VARBINARY", 32704);
					//dataType.Type.Length == null ? "VARBINARY" : $"VARBINARY({(dataType.Type.Length == 0 ? 1 : dataType.Type.Length)})";
				case DataType.Char:
					return "CHAR";
				case DataType.Date:
					return "DATE";
				case DataType.UInt64:
					return "DECIMAL(28,0)";
				case DataType.Decimal:
					return "DECIMAL";
				case DataType.Double:
					return "DOUBLE";
				case DataType.UInt16:
				case DataType.Int32:
					return "INTEGER";
				case DataType.Single:
					return "REAL";
				case DataType.Int16:
				case DataType.Boolean:
				case DataType.Byte:
					return "SMALLINT";
				case DataType.Time:
				case DataType.DateTimeOffset:
					return "TIME";
				case DataType.Timestamp:
				case DataType.DateTime:
				case DataType.DateTime2:
					return "TIMESTAMP"; //add precision 1..12 default 6
				case DataType.VarChar:
					return appendLength(dataType, "VARCHAR", 32704);
					//$"VARCHAR({(dataType.Type.Length == 0 ? 1 : dataType.Type.Length)})";
				case DataType.NVarChar:
					return appendLength(dataType, "NVARCHAR", 32704);
					//return $"NVARCHAR({(dataType.Type.Length == 0 ? 1 : dataType.Type.Length)})";
				case DataType.Guid:
					return mapGuidAsString ? "CHAR(32)" : "char(16) for bit data";
				default:
					return dataType.Type.DataType.ToString();
			}
		}

		//Why nop when statement is update?
		protected override void BuildFromClause(SqlStatement statement, SelectQuery selectQuery)
		{
			if (!statement.IsUpdate())
				base.BuildFromClause(statement, selectQuery);
		}

		//Same as base but handles wrapping values in a cast
		protected override void BuildInsertOrUpdateQueryAsMerge(SqlInsertOrUpdateStatement insertOrUpdate, string fromDummyTable)
		{
			var table = insertOrUpdate.Insert.Into;
			var targetAlias = Convert(new StringBuilder(), insertOrUpdate.SelectQuery.From.Tables[0].Alias, ConvertType.NameToQueryTableAlias).ToString();
			var sourceAlias = Convert(new StringBuilder(), GetTempAliases(1, "s")[0], ConvertType.NameToQueryTableAlias).ToString();
			var keys = insertOrUpdate.Update.Keys;

			AppendIndent().Append("MERGE INTO ");
			BuildPhysicalTable(table, null);
			StringBuilder.Append(' ').AppendLine(targetAlias);

			AppendIndent().Append("USING (SELECT ");

			ExtractMergeParametersIfCannotCombine(insertOrUpdate, keys);

			for (var i = 0; i < keys.Count; i++)
			{
				var key = keys[i];
				var expr = key.Expression;

				if (MergeSourceValueTypeRequired(expr))
				{
					//var exprType = SqlDataType.GetDataType(expr.SystemType);
					//var asType =
					//	GetiSeriesType(exprType);

					//StringBuilder.Append("CAST(");
					//BuildExpression(expr, false, false);
					//StringBuilder.AppendFormat(" AS {0})", asType);
					BuildTypedExpression(SqlDataType.GetDataType(expr.SystemType), expr);
				}
				else
					BuildExpression(expr, false, false);


				StringBuilder.Append(" AS ");
				BuildExpression(key.Column, false, false);

				if (i + 1 < keys.Count)
					StringBuilder.Append(", ");
			}

			if (!string.IsNullOrEmpty(fromDummyTable))
				StringBuilder.Append(' ').Append(fromDummyTable);

			StringBuilder.Append(") ").Append(sourceAlias).AppendLine(" ON");

			AppendIndent().AppendLine("(");

			Indent++;

			for (var i = 0; i < keys.Count; i++)
			{
				var key = keys[i];

				AppendIndent();

				StringBuilder.Append(targetAlias).Append('.');
				BuildExpression(key.Column, false, false);

				StringBuilder.Append(" = ").Append(sourceAlias).Append('.');
				BuildExpression(key.Column, false, false);

				if (i + 1 < keys.Count)
					StringBuilder.Append(" AND");

				StringBuilder.AppendLine();
			}

			Indent--;

			AppendIndent().AppendLine(")");

			if (insertOrUpdate.Update.Items.Any())
			{
				AppendIndent().AppendLine("WHEN MATCHED THEN");

				Indent++;
				AppendIndent().AppendLine("UPDATE ");
				BuildUpdateSet(insertOrUpdate.SelectQuery, insertOrUpdate.Update);
				Indent--;
			}

			AppendIndent().AppendLine("WHEN NOT MATCHED THEN");

			Indent++;
			BuildInsertClause(insertOrUpdate, insertOrUpdate.Insert, "INSERT", false, false);
			Indent--;

			while (EndLine.Contains(StringBuilder[StringBuilder.Length - 1]))
				StringBuilder.Length--;
		}

		//Used to expose basicbuilders method to 7.2 subclass - merging versions will remove this
		protected void DefaultBuildSqlMethod()
		{
			base.BuildSql();
		}


		//Test like conversion not needed
		protected override void BuildPredicate(ISqlPredicate predicate)
		{
			var newpredicate = predicate;

			switch (predicate.ElementType)
			{
				//probably does nothing
				case QueryElementType.LikePredicate:
					var p = (SqlPredicate.Like)predicate;

					var param2 = GetParm(p.Expr2 as IValueContainer, p.Expr1.SystemType);

					if (param2 != null)
					{
						if (param2 is SqlValue value && value.Value == null)
						{
							if (p.IsNot)
								newpredicate = new SqlPredicate.ExprExpr(p.Expr1, SqlPredicate.Operator.NotEqual, p.Expr2);
							else
								newpredicate = new SqlPredicate.ExprExpr(p.Expr1, SqlPredicate.Operator.Equal, p.Expr2);
						}
						else
							newpredicate = new SqlPredicate.Like(p.Expr1, p.IsNot, param2, p.Escape, p.IsSqlLike);
					}

					break;

				case QueryElementType.ExprExprPredicate:

					var ep = (SqlPredicate.ExprExpr)predicate;

					if (ep.Expr1 is SqlFunction function 
						&& function.Name == "Date"
						&& ep.Expr2 is SqlParameter parameter)
							parameter.Type = parameter.Type.WithDataType(DataType.Date);
						
					break;
			}

			base.BuildPredicate(newpredicate);
		}



		//Added by contributor - check
		protected override void BuildWhereClause(SelectQuery selectQuery)
		{
			if (!BuildWhere(selectQuery))
				return;

			this.StringBuilder.Append(' ');

			base.BuildWhereClause(selectQuery);
		}

		//Added by contributor - check
		protected override void BuildHavingClause(SelectQuery selectQuery)
		{
			if (selectQuery.Having.SearchCondition.Conditions.Count == 0)
				return;

			this.StringBuilder.Append(' ');

			base.BuildHavingClause(selectQuery);
		}

		//Added by contributor - check
		protected override void BuildOrderByClause(SelectQuery selectQuery)
		{
			if (selectQuery.OrderBy.Items.Count == 0)
				return;

			this.StringBuilder.Append(' ');

			base.BuildOrderByClause(selectQuery);
		}

		//Added by contributor - check
		protected override void BuildGroupByClause(SelectQuery selectQuery)
		{
			if (selectQuery.GroupBy.Items.Count == 0)
				return;

			this.StringBuilder.Append(' ');

			base.BuildGroupByClause(selectQuery);
		}

		// TODO: actually SystemType cannot be null in v3, so probably this method is not needed?
		private ISqlExpression GetParm(IValueContainer parameter, Type type)
		{
			if (type != null && parameter != null)
			{
				if (parameter is SqlValue value)
				{
					if (value.ValueType.SystemType == null)
						return new SqlValue(type, parameter.Value);
				}
				else if (parameter is SqlParameter p)
				{
					if (p.Type.SystemType == null)
						p.Type = p.Type.WithSystemType(type);
					return p;
				}
			}
			return null;
		}

		#endregion


		

		
		//DB2 - except it injects two extra conversions to expression
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

			if ((expr is SqlParameter parameter && parameter.Name != null)
				|| (expr is SqlValue value && value.Value == null))
			{

				var colType = GetiSeriesType(SqlDataType.GetDataType(expr.SystemType));
					expr = new SqlExpression(expr.SystemType, "Cast({0} as {1})", Precedence.Primary, expr, new SqlExpression(colType, Precedence.Primary));
			}

			if (wrap) StringBuilder.Append("CASE WHEN ");
			base.BuildColumnExpression(selectQuery, expr, alias, ref addAlias);
			if (wrap) StringBuilder.Append(" THEN 1 ELSE 0 END");
		}

		//Same as DB2 - except adds limit 1?
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
				var alia = selectQuery.Select.Columns.Select(c => c.Alias).ToArray();

				selectQuery.SetOperators.ForEach((u) =>
				{
					int colNo = 0;
					u.SelectQuery.Select.Columns
					.ForEach(c =>
					{
						c.Alias = alia[colNo];
						colNo++;
					});
				});
			}

			//DB2 impl + Fetch 1 ?
			if (selectQuery.From.Tables.Count == 0)
			{
				AppendIndent().AppendLine("SELECT");
				BuildColumns(selectQuery);
				AppendIndent().AppendLine($"FROM {DB2iSeriesTools.iSeriesDummyTableName()} FETCH FIRST 1 ROW ONLY");
			}
			else
			{
				base.BuildSelectClause(selectQuery);
			}
		}

		//Same as DB2  + quoteidentifier static
		public override StringBuilder Convert(StringBuilder sb, string value, ConvertType convertType)
		{
			switch (convertType)
			{
				case ConvertType.NameToQueryParameter:
					return sb.Append("@").Append(value);

				case ConvertType.NameToCommandParameter:
				case ConvertType.NameToSprocParameter:
					return sb.Append(":").Append(value);

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

			return sb.Append(value);
		}
		
		//Sames as DB2 - db2 also has special check for decimal
		protected override string GetProviderTypeName(IDbDataParameter parameter)
		{
			if (Provider != null)
			{
				var param = Provider.TryGetProviderParameter(parameter, MappingSchema);
				if (param != null)
					return Provider.Adapter.GetDbType(param).ToString();
			}

			return base.GetProviderTypeName(parameter);
		}

		//Same as base, except NULL - empty string
		protected override void BuildCreateTableNullAttribute(SqlField field, DefaultNullable defaulNullable)
		{
			if (defaulNullable == DefaultNullable.Null && field.CanBeNull)
				return;

			if (defaulNullable == DefaultNullable.NotNull && !field.CanBeNull)
				return;

			StringBuilder.Append(field.CanBeNull ? " " : "NOT NULL");
		}
		
		//Same as Base - except reversed first two steps
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
	}
}
