using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Diagnostics.CodeAnalysis;

namespace LinqToDB.Expressions
{
	using LinqToDB.Extensions;
	using Reflection;
	using Linq;
	using Linq.Builder;
	using Mapping;

	public static class InternalExtensionShims
	{
		public static bool IsSameGenericMethod(this MethodCallExpression method, MethodInfo genericMethodInfo)
		{
			if (!method.Method.IsGenericMethod)
				return false;
			return method.Method.GetGenericMethodDefinition() == genericMethodInfo;
		}

		public static Expression Unwrap(this Expression ex)
		{
			if (ex == null)
				return null;

			switch (ex.NodeType)
			{
				case ExpressionType.Quote:
				case ExpressionType.ConvertChecked:
				case ExpressionType.Convert:
					return ((UnaryExpression)ex).Operand.Unwrap();
			}

			return ex;
		}
	}
}
