using LinqToDB.Internal.DataProvider.Translation;
using LinqToDB.Internal.Extensions;
using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.SqlQuery;
using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesSqlExpressionConvertVisitor : SqlExpressionConvertVisitor
	{
		private static readonly string[] DB2LikeCharactersToEscape = ["%", "_"];

		public DB2iSeriesSqlExpressionConvertVisitor(bool allowModify) : base(allowModify)
		{
		}

		protected override bool SupportsNullInColumn => false;
		protected override bool ConcatRequiresExplicitStringCast => false;

		/// <inheritdoc />
		public override bool CanLowerIntervalDifference => true;

		/// <summary>
		/// The tick count is built from <c>MICROSECOND()</c> scaled by ten, so it is a multiple of ten by
		/// construction and a component below the microsecond is identically zero rather than merely imprecise.
		/// </summary>
		/// <remarks>
		/// Declared even though a column may be wider - <c>timestamp(7)</c> is what this fixture's model asks for -
		/// because what the measurement can distinguish is what the count is taken from, not what the column holds.
		/// The declined member falls back to .NET and answers from the two dates.
		/// </remarks>
		public override SqlIntervalUnit IntervalResolution => SqlIntervalUnit.Microsecond;

		/// <summary>
		/// Elapsed ticks summed from the three fields a DB2 timestamp decomposes into.
		/// </summary>
		/// <remarks>
		/// <c>TIMESTAMPDIFF</c> is documented as an estimate - it assumes months of thirty days - so it cannot
		/// answer this. A timestamp is a whole number of days plus seconds since midnight plus microseconds, and
		/// each field's difference is exact, so their sum is the exact elapsed time. This is the same
		/// decomposition the provider's <c>DateDiff</c> lowering already uses.
		/// </remarks>
		protected override ISqlExpression? ElapsedTicks(SqlIntervalDifferenceExpression element)
		{
			var longType = Factory.GetDbDataType(typeof(long));
			var intType = Factory.GetDbDataType(typeof(int));

			ISqlExpression FieldDifference(string function)
			{
				return Factory.Sub(longType,
					Factory.Cast(Factory.Function(intType, function, element.End), longType, true),
					Factory.Cast(Factory.Function(intType, function, element.Start), longType, true));
			}

			return Factory.Add(longType, Factory.Multiply(longType, FieldDifference("Days"), TimeSpan.TicksPerDay),
				Factory.Add(longType, Factory.Multiply(longType, FieldDifference("Midnight_Seconds"), TimeSpan.TicksPerSecond),
					Factory.Multiply(longType, FieldDifference("Microsecond"), TimeSpan.TicksPerMillisecond / 1000)));
		}

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

		/// <summary>
		/// Every ranking function, <c>NTILE</c>, and <c>LAG</c>/<c>LEAD</c> - all of them <c>SQL0104N ... Expected
		/// tokens may include: "ORDER BY"</c> - plus any frame, which is <c>SQL20117N A window specification for an
		/// OLAP function is not valid</c> on its own. <c>ROW_NUMBER</c> and the <c>*_VALUE</c> pair are fine
		/// unordered, and so is an unframed aggregate.
		/// </summary>
		protected override bool IsWindowOrderByRequired(SqlExtendedFunction func)
			=> func.FrameClause != null
				|| IsOrderDependentWindowFunction(func.FunctionName)
				|| func.FunctionName is "NTILE";
	}
}

