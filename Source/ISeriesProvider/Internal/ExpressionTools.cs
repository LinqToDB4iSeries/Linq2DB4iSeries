using System;
using System.Linq;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class ExpressionTools
	{
		public static readonly Expression<Func<string, string>> TrimStringExpression
				= x => ReaderExpressionTools.TrimString(x);

		public static LambdaExpressionBuilder<T> FromMethodInvocation<T>(Type type, string method)
		{
			var parameter = Expression.Parameter(type);
			var body = Expression.Call(parameter, type.GetMethod(method));
			return new LambdaExpressionBuilder<T>(body, new[] { parameter });
		}

		public static LambdaExpressionBuilder<T> FromMemberAccess<T>(Type type, string propertyOrField)
		{
			var parameter = Expression.Parameter(type);
			var body = Expression.PropertyOrField(parameter, propertyOrField);
			return new LambdaExpressionBuilder<T>(body, new[] { parameter });
		}

		public static LambdaExpressionBuilder<TResult> FromMemberAccess<T1, T2, TResult>(
			Type type, 
			string propertyOrField1, 
			string propertyOrField2,
			Expression<Func<T1, T2, TResult>> selectExpression)
		{
			var parameter = Expression.Parameter(type);
			var m1 = Expression.PropertyOrField(parameter, propertyOrField1);
			var m2 = Expression.PropertyOrField(parameter, propertyOrField2);
			var body = Expression.Invoke(selectExpression, m1, m2);
			return new LambdaExpressionBuilder<TResult>(body, new[] { parameter });
		}
	}

	internal class LambdaExpressionBuilder<T>
	{
		private readonly Expression expression;
		private readonly IEnumerable<ParameterExpression> parameters;

		public LambdaExpressionBuilder(Expression expression, IEnumerable<ParameterExpression> parameters)
		{
			this.expression = expression;
			this.parameters = parameters;
		}

		public LambdaExpressionBuilder<TResult> Pipe<TResult>(Expression<Func<T, TResult>> next) 
			=> new LambdaExpressionBuilder<TResult>(Expression.Invoke(next, expression), parameters);

		public LambdaExpression Build() 
			=> Expression.Lambda(
				Expression.GetDelegateType(parameters.Select(p => p.Type).Concat(new[] { typeof(T) }).ToArray()),
				expression,
				parameters);
	}
}
