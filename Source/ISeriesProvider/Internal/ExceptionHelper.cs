using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	static class ExceptionHelper
	{
		public static LinqToDBException InvalidProvider(IDataProvider provider)
			=> new LinqToDBException($"Unexpected provider type, DB2iSeriesDataProvider expect but was {provider.GetType()} instead.");

		public static LinqToDBException InvalidProviderName(string providerName)
			=> new LinqToDBException($"Invalid provider name {providerName}. Valid names are the constants defined in the DB2iSeriesProviderName class.");

		public static LinqToDBException InvalidAdoProvider(DB2iSeriesProviderType providerType)
			=> new LinqToDBException($"Invalid ADO.net provider type {providerType} for iSeries DataProvider.");

		public static LinqToDBException InvalidAssemblyName(string assemblyName)
			=> new LinqToDBException($"Invalid DB2 iSeries ADO.net provider assembly name {assemblyName}.");

		public static LinqToDBException InvalidProviderAdapter(IDynamicProviderAdapter adapter)
			=> new LinqToDBException($"Unexpcted provider adapter of type {adapter.GetType()}.");

		public static LinqToDBException InvalidConnectionString()
			=> new LinqToDBException("Provided connection string doesn't seem to be a valid DB2iSeries connection string.");

		public static LinqToDBException ConnectionStringParsingFailure(Exception e)
			=> new LinqToDBException($"Error while trying to detect DB2iSeries provider from connection string: {e.Message}", e);

		public static LinqToDBException InvalidDbConnectionType(IDbConnection connection)
			=> new LinqToDBException($"Underlying DbConnection of type {connection.GetType()} is not a supported type.");
	}
}
