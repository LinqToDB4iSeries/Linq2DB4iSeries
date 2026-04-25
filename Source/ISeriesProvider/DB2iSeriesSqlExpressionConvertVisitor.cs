using LinqToDB.Internal.Extensions;
using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.SqlQuery;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesSqlExpressionConvertVisitor : SqlExpressionConvertVisitor
	{
		private static readonly string[] DB2LikeCharactersToEscape = ["%", "_"];

		public DB2iSeriesSqlExpressionConvertVisitor(bool allowModify) : base(allowModify)
		{
		}

		protected override bool SupportsNullInColumn => false;

		public override string[] LikeCharactersToEscape => DB2LikeCharactersToEscape;

		public override ISqlExpression ConvertSqlUnaryExpression(SqlUnaryExpression element)
		{
			if (element.Operation is SqlUnaryOperation.BitwiseNegation)
				return new SqlFunction(element.Type, "BITNOT", element.Expr);

			return base.ConvertSqlUnaryExpression(element);
		}

		public override IQueryElement ConvertSqlBinaryExpression(SqlBinaryExpression element)
		{
			switch (element.Operation)
			{
				case "%":
					{
						var expr1 = !element.Expr1.SystemType!.IsIntegerType ? new SqlFunction(MappingSchema.GetDbDataType(typeof(int)), "Int", element.Expr1) : element.Expr1;
						return new SqlFunction(element.Type, "Mod", expr1, element.Expr2);
					}
				case "&": return new SqlFunction(element.Type, "BitAnd", element.Expr1, element.Expr2);
				case "|": return new SqlFunction(element.Type, "BitOr", element.Expr1, element.Expr2);
				case "^": return new SqlFunction(element.Type, "BitXor", element.Expr1, element.Expr2);
				case "+": return element.SystemType == typeof(string) ? new SqlBinaryExpression(element.SystemType, element.Expr1, "||", element.Expr2, element.Precedence) : element;
			}

			return base.ConvertSqlBinaryExpression(element);
		}


		public override ISqlExpression ConvertSqlFunction(SqlFunction func)
		{
			switch (func.Name)
			{
				case "Convert":
					//Conversion when target type is expressed as SqlDataType
					if (func.Parameters[0] is SqlDataType sqlType)
					{
						var type = sqlType.Type;
						if (type.SystemType == typeof(string) && func.Parameters[1].SystemType != typeof(string))
						{
							return new SqlFunction(func.Type, "RTrim", new SqlFunction(MappingSchema.GetDbDataType(typeof(string)), "Char", func.Parameters[1]));
						}
						else if (type.Length > 0)
						{
							return new SqlFunction(func.Type, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Length));
						}
						else if (type.Precision > 0 && type.Scale > 0)
						{
							return new SqlFunction(func.Type, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Precision), new SqlValue(type.Scale));
						}
						else if (type.Precision > 0)
						{
							return new SqlFunction(func.Type, type.DataType.ToString(), func.Parameters[1], new SqlValue(type.Precision));
						}
						else
						{
							return new SqlFunction(func.Type, type.DataType.ToString(), func.Parameters[1]);
						}
					}
					//Conversion when target type is expressed as pseudofunction e.g. Decimal(10)
					if (func.Parameters[0] is SqlFunction f)
					{
						//Conversion is setup with the datatype as the left operand. Character datatypes are presented as 
						//functions e.g. VarChar(1000). DB2 has a convert function for almost all datatypes named after the type.
						//So Linq2db Convert(VarChar(1000),SomeValue) needs to be converted to VarChar(SomeValue)
						if (f.Name == "Char" || f.Name == "Graphic" || f.Name == "VarChar" || f.Name == "VarGraphic")
							return new SqlFunction(func.Type, f.Name, func.Parameters[1]);

						if (f.Parameters.Length == 1)
							return new SqlFunction(func.Type, f.Name, func.Parameters[1], f.Parameters[0]);

						return new SqlFunction(func.Type, f.Name, func.Parameters[1], f.Parameters[0], f.Parameters[1]);
					}
					//Conversion when target type is expressed as string
					if (func.Parameters[0] is SqlExpression e)
						return new SqlFunction(func.Type, e.Expr, func.Parameters[1]);
					break;
				//Transform all datatype conversions to datatype functions
				case "Millisecond":
					return Div(new SqlFunction(func.Type, "Microsecond", func.Parameters), 1000);
				case "SmallDateTime":
				case "DateTime":
				case "DateTime2":
					return new SqlFunction(func.Type, "TimeStamp", func.Parameters);
				case "UInt16":
					return new SqlFunction(func.Type, "Int", func.Parameters);
				case "UInt32":
					return new SqlFunction(func.Type, "BigInt", func.Parameters);
				case "UInt64":
					return new SqlFunction(func.Type, "Decimal", func.Parameters);
				case "Byte":
				case "SByte":
				case "Int16":
					return new SqlFunction(func.Type, "SmallInt", func.Parameters);
				case "Int32":
					return new SqlFunction(func.Type, "Int", func.Parameters);
				case "Int64":
					return new SqlFunction(func.Type, "BigInt", func.Parameters);
				case "Double":
					return new SqlFunction(func.Type, "Float", func.Parameters);
				case "Single":
					return new SqlFunction(func.Type, "Real", func.Parameters);
				case "Money":
					return new SqlFunction(func.Type, "Decimal", func.Parameters[0], new SqlValue(19), new SqlValue(4));
				case "SmallMoney":
					return new SqlFunction(func.Type, "Decimal", func.Parameters[0], new SqlValue(10), new SqlValue(4));
				case "NChar":
				case "NVarChar":
					return new SqlFunction(func.Type, "Graphic", func.Parameters);
				//SqlValue parameter check to distinguish between Decimal datatype pseudofunction and actual conversion function
				case "Decimal" when func.Parameters.Length == 1 && func.Parameters[0] is not SqlValue:
					return new SqlFunction(func.Type, "Decimal", func.Parameters[0], new SqlValue(DB2iSeriesDbTypes.DbDecimal.DefaultPrecision!), new SqlValue(DB2iSeriesDbTypes.DbDecimal.DefaultScale!));
			}

			return base.ConvertSqlFunction(func);
		}

		public override ISqlExpression ConvertSqlExpression(SqlExpression element)
		{
			// Convert window function predicates to CASE WHEN
			if (element.IsWindowFunction)
			{
				var i = 0;
				foreach (var p in element.Parameters)
				{
					if (p is SqlSearchCondition searchCondition)
					{
						element.Parameters[i++] = WrapBooleanExpression(searchCondition, true, true);
					}
				}
			}

			return base.ConvertSqlExpression(element);
		}
	}
}
