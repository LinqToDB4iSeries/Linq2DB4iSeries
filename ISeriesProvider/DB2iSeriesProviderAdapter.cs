using System;
using System.Data;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public class DB2iSeriesProviderAdapter : IDynamicProviderAdapter
	{
		public IDynamicProviderAdapter WrappedAdapter => adapter;

		private readonly IDynamicProviderAdapter adapter;

		public DB2iSeriesAdoProviderType ProviderType { get; }

		public DB2iSeriesProviderAdapter(DB2iSeriesAdoProviderType providerType)
		{
			adapter = providerType switch
			{
				DB2iSeriesAdoProviderType.AccessClient => DB2iSeriesAccessClientProviderAdapter.GetInstance(),
				DB2iSeriesAdoProviderType.Odbc => OdbcProviderAdapter.GetInstance(),
				DB2iSeriesAdoProviderType.OleDb => OleDbProviderAdapter.GetInstance(),
				DB2iSeriesAdoProviderType.DB2 => DB2.DB2ProviderAdapter.GetInstance(),
				_ => throw ExceptionHelper.InvalidAdoProvider(providerType)
			};
			this.ProviderType = providerType;
		}

		public Type ConnectionType => adapter.ConnectionType;

		public Type DataReaderType => adapter.DataReaderType;

		public Type ParameterType => adapter.ParameterType;

		public Type CommandType => adapter.CommandType;

		public Type TransactionType => adapter.TransactionType;

		public string AssemblyName => ProviderType switch
		{
			DB2iSeriesAdoProviderType.AccessClient => DB2iSeriesAccessClientProviderAdapter.AssemblyName,
			DB2iSeriesAdoProviderType.Odbc => OdbcProviderAdapter.AssemblyName,
			DB2iSeriesAdoProviderType.OleDb => OleDbProviderAdapter.AssemblyName,
			DB2iSeriesAdoProviderType.DB2 => DB2.DB2ProviderAdapter.AssemblyName,
			_ => throw ExceptionHelper.InvalidAdoProvider(ProviderType)
		};


		public string GetDbTypeName(IDbDataParameter dbDataParameter) => adapter switch
		{
			DB2iSeriesAccessClientProviderAdapter accessClientAdapter => accessClientAdapter.GetDbType(dbDataParameter).ToString(),
			OdbcProviderAdapter odbcAdapter => odbcAdapter.GetDbType(dbDataParameter).ToString(),
			OleDbProviderAdapter oleDbAdapter => oleDbAdapter.GetDbType(dbDataParameter).ToString(),
			DB2.DB2ProviderAdapter db2Adapter => db2Adapter.GetDbType(dbDataParameter).ToString(),
			_ => throw ExceptionHelper.InvalidProviderAdapter(adapter)
		};

		public void SetDbType(IDbDataParameter dbDataParameter, object value)
		{
			if (adapter is DB2iSeriesAccessClientProviderAdapter accessClientAdapter
					&& value is DB2iSeriesAccessClientProviderAdapter.iDB2DbType idb2Type)
				accessClientAdapter.SetDbType(dbDataParameter, idb2Type);
			else if (adapter is DB2.DB2ProviderAdapter db2Adapter
					&& value is DB2.DB2ProviderAdapter.DB2Type db2Type)
				db2Adapter.SetDbType(dbDataParameter, db2Type);
			else if (adapter is OdbcProviderAdapter odbcAdapter
					&& value is OdbcProviderAdapter.OdbcType odbcType)
				odbcAdapter.SetDbType(dbDataParameter, odbcType);
			else if (adapter is OleDbProviderAdapter oleDbAdapter
					&& value is OleDbProviderAdapter.OleDbType oleDbType)
				oleDbAdapter.SetDbType(dbDataParameter, oleDbType);
			else
				throw ExceptionHelper.InvalidProviderAdapter(adapter);
		}

		public IDynamicProviderAdapter GetInstance()
			=> adapter;

		public static DB2iSeriesProviderAdapter GetInstance(DB2iSeriesAdoProviderType providerType)
			=> new DB2iSeriesProviderAdapter(providerType);


	}
}
