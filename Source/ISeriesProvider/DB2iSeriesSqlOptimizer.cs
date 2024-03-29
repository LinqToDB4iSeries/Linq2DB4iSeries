﻿using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Extensions;
	using LinqToDB.Mapping;
	using LinqToDB.SqlQuery;
	using SqlProvider;
	using System.Collections.Generic;

	class DB2iSeriesSqlOptimizer : BasicSqlOptimizer
	{
		private readonly DB2iSeriesSqlProviderFlags db2ISeriesSqlProviderFlags;
		private readonly DataOptions dataOptions;

		public DB2iSeriesSqlOptimizer(SqlProviderFlags sqlProviderFlags, DB2iSeriesSqlProviderFlags db2iSeriesSqlProviderFlags, DataOptions dataOptions)
			: base(sqlProviderFlags)
		{
			db2ISeriesSqlProviderFlags = db2iSeriesSqlProviderFlags;
			this.dataOptions = dataOptions;
		}

		public override bool CanCompareSearchConditions => true;

		protected static string[] DB2LikeCharactersToEscape = { "%", "_" };

		public override string[] LikeCharactersToEscape => DB2LikeCharactersToEscape;

		public override SqlStatement TransformStatement(SqlStatement statement, DataOptions dataOptions)
		{
			statement = SeparateDistinctFromPagination(statement, q => q.Select.SkipValue != null);
			statement = ReplaceDistinctOrderByWithRowNumber(statement, q => q.Select.SkipValue != null);

			if (!db2ISeriesSqlProviderFlags.SupportsOffsetClause)
				statement = ReplaceTakeSkipWithRowNumber(SqlProviderFlags, statement,
					static (SqlProviderFlags, query) => query.Select.SkipValue != null
					&& SqlProviderFlags.GetIsSkipSupportedFlag(query.Select.TakeValue, query.Select.SkipValue), true);

			return statement.QueryType switch
			{
				QueryType.Delete => GetAlternativeDelete((SqlDeleteStatement)statement, dataOptions),
				QueryType.Update => GetAlternativeUpdate((SqlUpdateStatement)statement, dataOptions),
				_ => statement,
			};
		}

		public override SqlStatement Finalize(MappingSchema mappingSchema, SqlStatement statement, DataOptions dataOptions)
		{
			static long getAbsoluteHashCode(object o)
				=> (long)o.GetHashCode() + (long)int.MaxValue;

			static string sanitizeAliasOrParameterName(string text, string alternative)
				=> !string.IsNullOrWhiteSpace(text) && text.All(x => x.IsLatinLetterOrNumber()) ?
					text : alternative;


			HashSet<object> visitedExpressions = new();

			void sanitizeNames(IQueryElement expr)
			{
				switch (expr)
				{
					case SqlParameter p:
						p.Name = sanitizeAliasOrParameterName(p.Name, $"P{getAbsoluteHashCode(p)}");
						break;
					case SqlTableSource table:
						table.Alias = sanitizeAliasOrParameterName(table.Alias, $"t{table.SourceID}");
						break;
					case SqlColumn column:
						column.Alias = sanitizeAliasOrParameterName(column.Alias, $"C{getAbsoluteHashCode(column)}");
						break;
					case SqlCteTable ctetable:
						//linq2db does not visit CteClause of SqlCteTable with a stack overflow possibility warning
						//this seems to trigger on recursive cte expressions, tracking visisted CteClauseExpressions to break recursiom
						if (!visitedExpressions.Contains(ctetable.Cte))
						{
							visitedExpressions.Add(ctetable.Cte);
							new QueryVisitor<object>(true, sanitizeNames).Visit(ctetable.Cte);
						}
						break;
				}
			}

			statement.VisitAll(sanitizeNames);

			return base.Finalize(mappingSchema, statement, dataOptions);
		}
		public override ISqlExpression ConvertExpressionImpl(ISqlExpression expression, ConvertVisitor<RunOptimizationContext> visitor)
		{
			expression = base.ConvertExpressionImpl(expression, visitor);
			if (expression is SqlBinaryExpression be)
			{
				switch (be.Operation)
				{
					case "%":
						var expr1 = !be.Expr1.SystemType.IsIntegerType() ? new SqlFunction(typeof(int), "Int", be.Expr1) : be.Expr1;
						return new SqlFunction(be.SystemType, "Mod", expr1, be.Expr2);
					case "&":
						return new SqlFunction(be.SystemType, "BitAnd", be.Expr1, be.Expr2);
					case "|":
						return new SqlFunction(be.SystemType, "BitOr", be.Expr1, be.Expr2);
					case "^":
						return new SqlFunction(be.SystemType, "BitXor", be.Expr1, be.Expr2);
					case "+":
						return be.SystemType == typeof(string) ? new SqlBinaryExpression(be.SystemType, be.Expr1, "||", be.Expr2, be.Precedence) : expression;
				}
			}
			else if (expression is SqlFunction func)
			{
				switch (func.Name)
				{
					case "EXISTS":
						return AlternativeExists(func);
					case "Convert":
						//Conversion to bool
						if (func.SystemType.ToUnderlying() == typeof(bool))
						{
							var ex = AlternativeConvertToBoolean(func, dataOptions, 1);
							if (ex != null)
							{
								return ex;
							}
						}
						//Conversion when target type is expressed as SqlDataType
						if (func.Parameters[0] is SqlDataType sqlType)
						{
							var type = sqlType.Type;
							if (type.SystemType == typeof(string) && func.Parameters[1].SystemType != typeof(string))
							{
								return new SqlFunction(func.SystemType, "RTrim", new SqlFunction(typeof(string), "Char", func.Parameters[1]));
							}
							else if (type.Length > 0)
							{
								return new SqlFunction(func.SystemType, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Length));
							}
							else if (type.Precision > 0 && type.Scale > 0)
							{
								return new SqlFunction(func.SystemType, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Precision), new SqlValue(type.Scale));
							}
							else if (type.Precision > 0)
							{
								return new SqlFunction(func.SystemType, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Precision));
							}
							else
							{
								return new SqlFunction(func.SystemType, type.DataType.ToString(), func.Parameters[1]);
							}
						}
						//Conversion when target type is expressed as pseudofunction e.g. Decimal(10)
						if (func.Parameters[0] is SqlFunction f)
						{
							//Conversion is setup with the datatype as the left operand. Character datatypes are presented as 
							//functions e.g. VarChar(1000). DB2 has a convert function for almost all datatypes named after the type.
							//So Linq2db Convert(VarChar(1000),SomeValue) needs to be converted to VarChar(SomeValue)
							if (f.Name == "Char" || f.Name == "Graphic" || f.Name == "VarChar" || f.Name == "VarGraphic")
								return new SqlFunction(func.SystemType, f.Name, func.Parameters[1]);

							if (f.Parameters.Length == 1)
								return new SqlFunction(func.SystemType, f.Name, func.Parameters[1], f.Parameters[0]);

							return new SqlFunction(func.SystemType, f.Name, func.Parameters[1], f.Parameters[0], f.Parameters[1]);
						}
						//Conversion when target type is expressed as string
						if (func.Parameters[0] is SqlExpression e)
							return new SqlFunction(func.SystemType, e.Expr, func.Parameters[1]);
						break;
					//Transform all datatype conversions to datatype functions
					case "Millisecond":
						return Div(new SqlFunction(func.SystemType, "Microsecond", func.Parameters), 1000);
					case "SmallDateTime":
					case "DateTime":
					case "DateTime2":
						return new SqlFunction(func.SystemType, "TimeStamp", func.Parameters);
					case "UInt16":
						return new SqlFunction(func.SystemType, "Int", func.Parameters);
					case "UInt32":
						return new SqlFunction(func.SystemType, "BigInt", func.Parameters);
					case "UInt64":
						return new SqlFunction(func.SystemType, "Decimal", func.Parameters);
					case "Byte":
					case "SByte":
					case "Int16":
						return new SqlFunction(func.SystemType, "SmallInt", func.Parameters);
					case "Int32":
						return new SqlFunction(func.SystemType, "Int", func.Parameters);
					case "Int64":
						return new SqlFunction(func.SystemType, "BigInt", func.Parameters);
					case "Double":
						return new SqlFunction(func.SystemType, "Float", func.Parameters);
					case "Single":
						return new SqlFunction(func.SystemType, "Real", func.Parameters);
					case "Money":
						return new SqlFunction(func.SystemType, "Decimal", func.Parameters[0], new SqlValue(19), new SqlValue(4));
					case "SmallMoney":
						return new SqlFunction(func.SystemType, "Decimal", func.Parameters[0], new SqlValue(10), new SqlValue(4));
					case "NChar":
					case "NVarChar":
						return new SqlFunction(func.SystemType, "Graphic", func.Parameters);
					//SqlValue parameter check to distinguish between Decimal datatype pseudofunction and actual conversion function
					case "Decimal" when func.Parameters.Length == 1 && func.Parameters[0] is not SqlValue:
						return new SqlFunction(func.SystemType, "Decimal", func.Parameters[0], new SqlValue(DB2iSeriesDbTypes.DbDecimal.DefaultPrecision), new SqlValue(DB2iSeriesDbTypes.DbDecimal.DefaultScale));
				}
			}
			// Transform SqlSearchCondition
			else if (expression is SqlSearchCondition search)
			{
				// This list will contain transformed conditions and will only be used if a new SqlSearchCondition is returned.
				List<SqlCondition> conditions = new();
				bool buildNew = false;

				// Tranform the conditions within a SqlSearchCondition
				for (int i = 0; i < search.Conditions.Count; i++)
				{
					var condition = search.Conditions[i];

					// A predicate cannot directly compare two search expressions.
					// DB2i has no boolean type, so expressions like `a = b` do not return booleans.
					// Instead they are typeless expressions.
					// Expressions like `(a = b) = false` are invalid because an expression cannot be compared to value.
					// Expressions like `(a = b) = (c = d)` are also invalid because an expression cannot be compared to another expression.
					// Instead, nested search conditions must be placed inside case statements that returns numbers representing true and false.
					// The expression above becomes (case when a = b then 1 else 0 end) = (case when c = d then 1 else 0 end).
					if (condition.Predicate is SqlPredicate.ExprExpr predicate
						&& (predicate.Expr1.ElementType == QueryElementType.SearchCondition
						|| predicate.Expr2.ElementType == QueryElementType.SearchCondition))
					{
						var expr1 = predicate.Expr1;
						var expr2 = predicate.Expr2;

						// If expr1 is a SqlSearchCondition, wrap it in a case statement.
						if (expr1.ElementType == QueryElementType.SearchCondition)
						{
							expr1 = new SqlFunction(typeof(bool), "CASE", new ISqlExpression[] {
								expr1,
								new SqlValue(true),
								new SqlValue(false)
							});
						}

						// If expr2 is a SqlSearchCondition, wrap it in a case statement.
						if (expr2.ElementType == QueryElementType.SearchCondition)
						{
							expr2 = new SqlFunction(typeof(bool), "CASE", new ISqlExpression[] {
								expr2,
								new SqlValue(true),
								new SqlValue(false)
							});
						}

						// Build the new predicate.
						var newPredicate = new SqlPredicate.ExprExpr(expr1, predicate.Operator, expr2, predicate.WithNull);
						var newCondition = new SqlCondition(condition.IsNot, newPredicate, condition.IsOr);
						conditions.Add(newCondition);

						// Set the flag to build a new SqlSearchCondition using the new conditions.
						buildNew = true;
					}
					else if (condition.Predicate is SqlPredicate.ExprExpr p2
						&& (p2.Expr1.ElementType == QueryElementType.SqlParameter
						&& p2.Expr2.ElementType == QueryElementType.SqlField
						|| p2.Expr1.ElementType == QueryElementType.SqlField
						&& p2.Expr2.ElementType == QueryElementType.SqlParameter))
					{
						var fieldType = p2.Expr1.ElementType switch
						{
							QueryElementType.SqlField => ((SqlField)p2.Expr1).Type,
							_ => ((SqlField)p2.Expr2).Type
						};

						var param = p2.Expr1.ElementType switch
						{
							QueryElementType.SqlParameter => (SqlParameter)p2.Expr1,
							_ => (SqlParameter)p2.Expr2
						};

						if (param.Type.DataType == DataType.Undefined)
						{
							param.Type = param.Type.WithDataType(fieldType.DataType)
								.WithScale(fieldType.Scale)
								.WithLength(fieldType.Length)
								.WithPrecision(fieldType.Precision);
						}

						conditions.Add(condition);
					}
					else
					{
						// Untransformed conditions should be kept.
						conditions.Add(condition);
					}
				}

				if (buildNew)
				{
					// We must return a new SqlSearchCondition.
					// Modifying the existing one will break the library because the result is check for reference equality.
					return new SqlSearchCondition(conditions);
				}
			}
			return expression;
		}

		protected ISqlExpression AlternativeExists(SqlFunction func)
		{
			var query = (SelectQuery)func.Parameters[0];

			if (query.Select.Columns.Count == 0)
				query.Select.Columns.Add(new SqlColumn(query, new SqlExpression("'.'")));

			query.Select.Take(1, null);

			var sc = new SqlSearchCondition();

			sc.Conditions.Add(
				new SqlCondition(false, new SqlPredicate.IsNull(query, true)));

			return sc;
		}

		protected override ISqlExpression ConvertFunction(SqlFunction func)
		{
			func = ConvertFunctionParameters(func, false);
			return base.ConvertFunction(func);
		}
	}
}
