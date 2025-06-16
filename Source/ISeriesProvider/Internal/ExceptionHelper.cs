using System;
using System.Data;
using LinqToDB.Internal.DataProvider;

namespace LinqToDB.DataProvider.DB2iSeries
{
	static class ExceptionHelper
	{
		public static LinqToDBException InvalidProvider(IDataProvider provider)
			=> new($"Unexpected provider type, DB2iSeriesDataProvider expect but was {provider.GetType()} instead.");

		public static LinqToDBException InvalidProviderName(string providerName)
			=> new($"Invalid provider name {providerName}. Valid names are the constants defined in the DB2iSeriesProviderName class.");

		public static LinqToDBException InvalidAdoProvider(DB2iSeriesProviderType providerType)
			=> new($"Invalid ADO.net provider type {providerType} for iSeries DataProvider.");

		public static LinqToDBException InvalidAssemblyName(string assemblyName)
			=> new($"Invalid DB2 iSeries ADO.net provider assembly name {assemblyName}.");

		public static LinqToDBException InvalidProviderAdapter(IDynamicProviderAdapter adapter)
			=> new($"Unexpcted provider adapter of type {adapter.GetType()}.");

		public static LinqToDBException InvalidConnectionString()
			=> new("Provided connection string doesn't seem to be a valid DB2iSeries connection string.");

		public static LinqToDBException ConnectionStringParsingFailure(Exception e)
			=> new($"Error while trying to detect DB2iSeries provider from connection string: {e.Message}", e);

		public static LinqToDBException InvalidDbConnectionType(IDbConnection connection)
			=> new($"Underlying DbConnection of type {connection.GetType()} is not a supported type.");

		public static LinqToDBException MisssingDbConnection()
			=> new($"DbConnection cannot be retrieved from DataConnection.");

		public static LinqToDBException CouldNotDetectProvider()
			=> new("Could not detect DB2iSeries provider from connection options.");
	}
}
