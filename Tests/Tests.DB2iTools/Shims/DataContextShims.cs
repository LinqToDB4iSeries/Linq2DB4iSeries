using LinqToDB;
using LinqToDB.Data;
using System.Reflection;

namespace Tests
{
	public static class DataContextShims
	{
		public static DataConnection GetDataConnection(this DataContext dataContext)
		{
			return (DataConnection)dataContext.GetType()
				.GetMethod(nameof(GetDataConnection), BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
				.Invoke(dataContext, new object[] { });
		}
	}
}
