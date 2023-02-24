using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class EmptyArray<T>
	{
#if NET45
		public static readonly T[] Value = new T[0];
#else
		public static readonly T[] Value = Array.Empty<T>();
#endif
	}
}
