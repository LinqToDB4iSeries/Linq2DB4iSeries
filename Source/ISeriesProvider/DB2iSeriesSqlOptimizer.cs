using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	class DB2iSeriesSqlOptimizer : BasicSqlOptimizer
	{
		private readonly DB2iSeriesSqlProviderFlags db2ISeriesSqlProviderFlags;
		private readonly DataOptions dataOptions;
		private readonly DB2iSeriesProviderType providerType;

		public DB2iSeriesSqlOptimizer(SqlProviderFlags sqlProviderFlags, DB2iSeriesSqlProviderFlags db2iSeriesSqlProviderFlags, DataOptions dataOptions, DB2iSeriesProviderType providerType)
			: base(sqlProviderFlags)
		{
			db2ISeriesSqlProviderFlags = db2iSeriesSqlProviderFlags;
			this.dataOptions = dataOptions;
			this.providerType = providerType;
		}

		protected static string[] DB2LikeCharactersToEscape = ["%", "_"];
		
		public override SqlStatement TransformStatement(SqlStatement statement, DataOptions dataOptions, MappingSchema mappingSchema)
		{
			statement = SeparateDistinctFromPagination(statement, q => q.Select.SkipValue != null);
			
			if (!db2ISeriesSqlProviderFlags.SupportsOffsetClause)
				statement = ReplaceTakeSkipWithRowNumber(SqlProviderFlags, statement, mappingSchema, 
					static (SqlProviderFlags, query) => query.Select.SkipValue != null
					&& SqlProviderFlags.GetIsSkipSupportedFlag(query.Select.TakeValue), true);
			
			return statement.QueryType switch
			{
				QueryType.Delete => GetAlternativeDelete((SqlDeleteStatement)statement),
				QueryType.Update => GetAlternativeUpdate((SqlUpdateStatement)statement, dataOptions, mappingSchema),
				_ => statement,
			};
		}

		public override SqlStatement Finalize(MappingSchema mappingSchema, SqlStatement statement, DataOptions dataOptions)
		{
			static long getAbsoluteHashCode(object o)
				=> (long)o.GetHashCode() + (long)int.MaxValue;

			static string? sanitizeAliasOrParameterName(string? text, string alternative)
				=> !string.IsNullOrWhiteSpace(text) && text.All(x => x.IsLatinLetterOrNumber()) ?
					text : alternative;


			HashSet<object> visitedExpressions = [];

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
					case SqlCteTable ctetable when ctetable.Cte is not null:
						//linq2db does not visit CteClause of SqlCteTable with a stack overflow possibility warning
						//this seems to trigger on recursive cte expressions, tracking visisted CteClauseExpressions to break recursiom
						if (!visitedExpressions.Contains(ctetable.Cte))
						{
							visitedExpressions.Add(ctetable.Cte);
							ctetable.Cte.VisitAll(sanitizeNames);
						}
						break;
				}
			}

			// The upstream SQL builder renders every format argument, registering parameters
			// even when no placeholder references them. Keep indexes stable, but prevent
			// unused arguments from contributing parameters to the generated command.
			statement.VisitAll(static element =>
			{
				var (format, parameters) = element switch
				{
					SqlExpression expression => (expression.Expr, expression.Parameters),
					SqlRawSqlTable table     => (table.SQL, table.Parameters),
					_                        => (null, null),
				};

				// Literal positional markers can bind arguments without a {n} placeholder.
				if (format == null || parameters == null || parameters.Length == 0 || format.IndexOf('?') >= 0)
					return;

				var referenced = new bool[parameters.Length];
				QueryHelper.TransformExpressionIndexes(referenced, format, static (used, index) =>
				{
					if (index >= 0 && index < used.Length)
						used[index] = true;
					return index;
				});

				for (var i = 0; i < parameters.Length; i++)
					if (!referenced[i])
						parameters[i] = new SqlFragment(string.Empty);
			});

			//Sanitize parameter names and table/column aliases  
			statement.VisitAll(sanitizeNames);

			return base.Finalize(mappingSchema, statement, dataOptions);
		}

		public override SqlStatement FinalizeStatement(SqlStatement statement, EvaluationContext context, DataOptions dataOptions, MappingSchema mappingSchema)
		{
			statement = base.FinalizeStatement(statement, context, dataOptions, mappingSchema);
			
			HandleOleDbDecimalOverflow(ref statement);
			
			return statement;
		}

		// OleDb reads DBTYPE_NUMERIC into a 96-bit coefficient before removing trailing
		// zeros. A set operation can widen the scale enough to overflow that coefficient
		// even when each value fits decimal. Convert only the final result to text, keeping
		// set comparisons, arithmetic, sorting and pagination in the numeric domain.
		private void HandleOleDbDecimalOverflow(ref SqlStatement statement)
		{
			if (!providerType.IsOleDb() || statement is not SqlSelectStatement select)
				return;

			var query = select.SelectQuery;
			var convert = query.Select.Columns.Select(NeedsDecimalText).ToArray();
			if (!convert.Any(static value => value))
				return;

			statement = QueryHelper.WrapQuery(statement, query, allowMutation: true, doNotRemove: true);
			var outer = statement.SelectQuery!;
			foreach (var item in query.OrderBy.Items)
			{
				var column = query.Select.Columns.FirstOrDefault(c => c.Expression.Equals(item.Expression));
				column ??= query.Select.AddNewColumn(item.Expression);
				outer.OrderBy.Items.Add(new SqlOrderByItem(column, item.IsDescending, item.IsPositioned, item.NullsPosition));
			}
			if (query.Select.TakeValue == null && query.Select.SkipValue == null)
				query.OrderBy.Items.Clear();

			for (var i = 0; i < convert.Length; i++)
				if (convert[i])
					outer.Select.Columns[i].Expression = new SqlFunction(new DbDataType(typeof(string), DataType.VarChar), "VARCHAR", outer.Select.Columns[i].Expression, new SqlValue("."));
			return;
		}

		static bool NeedsDecimalText(SqlColumn column)
		{
			if (column.SystemType != typeof(decimal) && column.SystemType != typeof(decimal?))
				return false;

			if (column.Parent is { HasSetOperators: true } query)
			{
				var index = query.Select.Columns.IndexOf(column);
				var integerDigits = 0;
				var scale = 0;

				void include(ISqlExpression expression)
				{
					// ABS preserves its operand's decimal facets.
					if (expression is SqlFunction { Name: "Abs" } function && function.Parameters.Length == 1)
						expression = function.Parameters[0];
					var type = QueryHelper.GetDbDataTypeWithoutSchema(expression);
					var precision = type.Precision ?? DB2iSeriesDbTypes.DbDecimal.DefaultPrecision!.Value;
					var operandScale = type.Scale ?? DB2iSeriesDbTypes.DbDecimal.DefaultScale!.Value;
					integerDigits = Math.Max(integerDigits, precision - operandScale);
					scale = Math.Max(scale, operandScale);
				}

				include(column.Expression);
				foreach (var operation in query.SetOperators)
					include(operation.SelectQuery.Select.Columns[index].Expression);
				if (integerDigits + scale > 28)
					return true;
			}

			return column.Expression is SqlColumn inner && NeedsDecimalText(inner);
		}

		public override SqlExpressionConvertVisitor CreateConvertVisitor(bool allowModify)
		{
			return new DB2iSeriesSqlExpressionConvertVisitor(allowModify);
		}

		public override SqlExpressionOptimizerVisitor CreateOptimizerVisitor(bool allowModify)
		{
			return base.CreateOptimizerVisitor(allowModify);
		}
	}
}
