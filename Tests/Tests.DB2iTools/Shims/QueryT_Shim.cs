using LinqToDB;
using LinqToDB.Linq;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Tests
{
	public static class Query<T>
	{
		private static readonly Lazy<Type> query = new Lazy<Type>(() => typeof(Query).Assembly.GetTypes()
				.First(x => x.Name.StartsWith(nameof(Query) + "`")
				&& x.IsGenericTypeDefinition
				&& x.GetGenericArguments().Count() == 1)
				.MakeGenericType(typeof(T)));
		
		public static Query GetQuery(IDataContext dataContext, ref Expression expression)
			=> (Query)query.Value
				.GetMethod(nameof(GetQuery), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(null, new object[] { dataContext, expression });
		
		public static void ClearCache()
			=> query.Value
				.GetMethod(nameof(ClearCache), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
				.Invoke(null, new object[] { });
		
		public static int CacheMissCount
			=> (int)query.Value
				.GetProperty(nameof(CacheMissCount))
				.GetValue(null);
	}
}
