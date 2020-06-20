namespace LinqToDB.DataProvider.DB2iSeries
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Data;
	using System.Data.Common;
	using System.Linq.Expressions;
	using System.Reflection;
	using LinqToDB.Expressions;
	using LinqToDB.Extensions;
	using LinqToDB.Mapping;

	// contains copy of APIs, that are internal in rc1 and will be exposed in final release
	public class InternalAPIs
	{
		// TODO: replace with Common.Tools.TryLoadAssembly
		internal static Assembly TryLoadAssembly(string assemblyName, string providerFactory)
		{
			if (assemblyName != null)
			{
				try
				{
					return Assembly.Load(assemblyName);
				}
				catch { }
			}

#if !NETSTANDARD2_0
			try
			{
				return DbProviderFactories.GetFactory(providerFactory).GetType().Assembly;
			}
			catch { }
#endif

			return null;
		}

		private readonly static IDictionary<Type, Func<IDbDataParameter, IDbDataParameter>> _parameterConverters = new ConcurrentDictionary<Type, Func<IDbDataParameter, IDbDataParameter>>();
		private readonly static IDictionary<Type, Func<IDbConnection, IDbConnection>> _connectionConverters = new ConcurrentDictionary<Type, Func<IDbConnection, IDbConnection>>();

		// method of provider (without 3-rd parameter)
		public static IDbDataParameter TryGetProviderParameter(IDbDataParameter parameter, MappingSchema ms, Type parameterType)
		{
			return TryConvertProviderType(_parameterConverters, parameterType, parameter, ms);
		}

		// method of provider (without 3-rd parameter)
		public static IDbConnection TryGetProviderConnection(IDbConnection connection, MappingSchema ms, Type connectionType)
		{
			return TryConvertProviderType(_connectionConverters, connectionType, connection, ms);
		}

		private static TResult TryConvertProviderType<TResult>(
			IDictionary<Type, Func<TResult, TResult>> converters,
			Type expectedType,
			TResult value,
			MappingSchema ms)
			where TResult : class
		{
			var valueType = value.GetType();

			if (expectedType.IsSameOrParentOf(valueType))
				return value;

			if (!converters.TryGetValue(valueType, out var converter))
			{
				// don't think it makes sense to lock creation of new converter
				var converterExpr = ms.GetConvertExpression(valueType, typeof(TResult), false, false);

				if (converterExpr != null)
				{
					var param = Expression.Parameter(typeof(TResult));
					converter = (Func<TResult, TResult>)Expression
						.Lambda(
							converterExpr.GetBody(Expression.Convert(param, valueType)),
							param)
						.Compile();

					converters[valueType] = converter;
				}
			}

			if (converter != null)
				return converter(value);

			return null;
		}
	}
}
