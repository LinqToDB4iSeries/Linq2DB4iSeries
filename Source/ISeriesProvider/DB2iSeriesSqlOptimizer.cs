using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.Mapping;
using System.Collections.Generic;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
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

			//Sanitize parameter names and table/column aliases  
			statement.VisitAll(sanitizeNames);

			return base.Finalize(mappingSchema, statement, dataOptions);
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
