using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class TypeCreatorNoDefault<T> : TypeCreatorBase
	{
		Func<T, object> _creator;

		public dynamic CreateInstance(T value)
		{
			return (_creator ?? (_creator = GetCreator<T>()))(value);
		}
	}
}
