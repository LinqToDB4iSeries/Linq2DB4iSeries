namespace LinqToDB.DataProvider.DB2iSeries
{
	using Extensions;
	using SqlProvider;
	using SqlQuery;
	class DB2iSeriesSqlOptimizer : BasicSqlOptimizer
	{

		public DB2iSeriesSqlOptimizer(SqlProviderFlags sqlProviderFlags) : base(sqlProviderFlags)
		{
		}

		private static void SetQueryParameter(IQueryElement element)
		{
			if (element.ElementType == QueryElementType.SqlParameter)
			{
				((SqlParameter)element).IsQueryParameter = false;
			}
		}

	    private static string FixUnderscore(string text)
	    {
	        if (text == null)
	            return null;

	        if (text.Equals("_"))
	            return "underscore_";
	            
	        return text.TrimStart('_');
        }

	    public override SqlStatement Finalize(SqlStatement statement)
	    {
            new QueryVisitor().Visit(statement, expr =>
            {
                switch (expr.ElementType)
                {
                    case QueryElementType.SqlParameter:
                        {
                            var p = (SqlParameter)expr;
                            p.Name = FixUnderscore(p.Name);
                            
                            break;
                        }
                    case QueryElementType.TableSource:
                        {
                            var table = (SqlTableSource)expr;
                            table.Alias = FixUnderscore(table.Alias);
                            break;
                        }
                    case QueryElementType.Column:
                        {
                            var column = (SqlColumn)expr;
                            column.Alias = FixUnderscore(column.Alias);
                            break;
                        }
                }
            });

            if (statement.SelectQuery != null)
				(new QueryVisitor()).Visit(statement.SelectQuery.Select, SetQueryParameter);

	        statement = base.Finalize(statement);

	        switch (statement.QueryType)
	        {
	            case QueryType.Delete:
	                return GetAlternativeDelete((SqlDeleteStatement)statement);
	            case QueryType.Update:
	                return GetAlternativeUpdate((SqlUpdateStatement)statement);
	            default:
	                return statement;
	        }
        }

		public override ISqlExpression ConvertExpression(ISqlExpression expr)
		{
			expr = base.ConvertExpression(expr);
			if (expr is SqlBinaryExpression)
			{
				var be = (SqlBinaryExpression)expr;
				switch (be.Operation)
				{
					case "%":
						if (true)
						{
							var expr1 = !be.Expr1.SystemType.IsIntegerType() ? new SqlFunction(typeof(int), "Int", be.Expr1) : be.Expr1;
							return new SqlFunction(be.SystemType, "Mod", expr1, be.Expr2);
						}
					case "&":
						return new SqlFunction(be.SystemType, "BitAnd", be.Expr1, be.Expr2);
					case "|":
						return new SqlFunction(be.SystemType, "BitOr", be.Expr1, be.Expr2);
					case "^":
						return new SqlFunction(be.SystemType, "BitXor", be.Expr1, be.Expr2);
					case "+":
						return be.SystemType == typeof(string) ? new SqlBinaryExpression(be.SystemType, be.Expr1, "||", be.Expr2, be.Precedence) : expr;
				}
			}
			else if (expr is SqlFunction)
			{
				var func = (SqlFunction)expr;
				switch (func.Name)
				{
					case "EXISTS":
						return AlternativeExists(func);
					case "Convert":
						if (func.SystemType.ToUnderlying() == typeof(bool))
						{
							dynamic ex = AlternativeConvertToBoolean(func, 1);
							if (ex != null)
							{
								return ex;
							}
						}
						if (func.Parameters[0] is SqlDataType)
						{
							dynamic type = (SqlDataType)func.Parameters[0];
							if (type.Type == typeof(string) && func.Parameters[1].SystemType != typeof(string))
							{
								return new SqlFunction(func.SystemType, "RTrim", new SqlFunction(typeof(string), "Char", func.Parameters[1]));
							}
							else if (type.Length > 0)
							{
								return new SqlFunction(func.SystemType, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Length));
							}
							else if (type.Precision > 0)
							{
								return new SqlFunction(func.SystemType, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Precision), new SqlValue(type.Scale));
							}
							else
							{
								return new SqlFunction(func.SystemType, type.DataType.ToString(), func.Parameters[1]);
							}
						}
						if (func.Parameters[0] is SqlFunction)
						{
							dynamic f = (SqlFunction)func.Parameters[0];
							if (f.Name == "Char")
								return new SqlFunction(func.SystemType, f.Name, func.Parameters[1]);

							if (f.Parameters.Length == 1)
								return new SqlFunction(func.SystemType, f.Name, func.Parameters[1], f.Parameters[0]);

							return new SqlFunction(func.SystemType, f.Name, func.Parameters[1], f.Parameters[0], f.Parameters[1]);
						}
						dynamic e = (SqlExpression)func.Parameters[0];
						return new SqlFunction(func.SystemType, e.Expr, func.Parameters[1]);
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
					case "VarChar":
						if (func.Parameters[0].SystemType.ToUnderlying() == typeof(decimal))
						{
							return new SqlFunction(func.SystemType, "Char", func.Parameters[0]);
						}
						break;
					case "NChar":
					case "NVarChar":
						return new SqlFunction(func.SystemType, "Char", func.Parameters);
					case "DateDiff":
						switch ((Sql.DateParts)((SqlValue)func.Parameters[0]).Value)
						{
							case Sql.DateParts.Day:
								return new SqlExpression(typeof(int), "((Days({0}) - Days({1})) * 86400 + (MIDNIGHT_SECONDS({0}) - MIDNIGHT_SECONDS({1}))) / 86400", Precedence.Multiplicative, func.Parameters[2], func.Parameters[1]);
							case Sql.DateParts.Hour:
								return new SqlExpression(typeof(int), "((Days({0}) - Days({1})) * 86400 + (MIDNIGHT_SECONDS({0}) - MIDNIGHT_SECONDS({1}))) / 3600", Precedence.Multiplicative, func.Parameters[2], func.Parameters[1]);
							case Sql.DateParts.Minute:
								return new SqlExpression(typeof(int), "((Days({0}) - Days({1})) * 86400 + (MIDNIGHT_SECONDS({0}) - MIDNIGHT_SECONDS({1}))) / 60", Precedence.Multiplicative, func.Parameters[2], func.Parameters[1]);
							case Sql.DateParts.Second:
								return new SqlExpression(typeof(int), "(Days({0}) - Days({1})) * 86400 + (MIDNIGHT_SECONDS({0}) - MIDNIGHT_SECONDS({1}))", Precedence.Additive, func.Parameters[2], func.Parameters[1]);
							case Sql.DateParts.Millisecond:
								return new SqlExpression(typeof(int), "((Days({0}) - Days({1})) * 86400 + (MIDNIGHT_SECONDS({0}) - MIDNIGHT_SECONDS({1}))) * 1000 + (MICROSECOND({0}) - MICROSECOND({1})) / 1000", Precedence.Additive, func.Parameters[2], func.Parameters[1]);
						}
						break;
				}
			}
			return expr;
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
	}
}
