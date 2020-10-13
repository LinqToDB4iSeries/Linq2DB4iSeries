using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;
using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class DB2iSeriesExtensions
	{
		public static string ToSqlString(this DbDataType dbDataType)
		{
			return DB2iSeriesSqlBuilder.GetDbType(dbDataType.DbType, dbDataType.Length, dbDataType.Precision, dbDataType.Scale);
		}

		public static SqlDataType GetTypeOrUnderlyingTypeDataType(this MappingSchema mappingSchema, Type type)
		{
			var sqlDataType = mappingSchema.GetDataType(type);
			if (sqlDataType.Type.DataType == DataType.Undefined)
				sqlDataType = mappingSchema.GetUnderlyingDataType(type, out var _);

			return sqlDataType.Type.DataType == DataType.Undefined ? SqlDataType.Undefined : sqlDataType;
		}

		public static bool IsGuidMappedAsString(this MappingSchema mappingSchema)
		{
			return mappingSchema is DB2iSeriesMappingSchemaBase iseriesMappingSchema
				&& iseriesMappingSchema.GuidMappedAsString;
		}

		public static DbDataType GetDbDataType(this MappingSchema mappingSchema, Type systemType, DataType dataType, int? length, int? precision, int? scale, bool mapGuidAsString, bool forceDefaultAttributes = false)
		{
			return DB2iSeriesDbTypes.GetDbDataType(systemType, dataType, length, precision, scale, mappingSchema.IsGuidMappedAsString(), forceDefaultAttributes);
		}

		public static DbDataType GetDbTypeForCast(this MappingSchema mappingSchema, SqlDataType type)
		{
			return DB2iSeriesDbTypes.GetDbTypeForCast(type, mappingSchema);
		}

		public static IDbConnection GetProviderConnection(this DataConnection dataConnection)
		{
			var connection = dataConnection.DataProvider switch
			{
				IDB2iSeriesDataProvider iSeriesDataProvider => iSeriesDataProvider.TryGetProviderConnection(dataConnection.Connection, dataConnection.MappingSchema, out var c) ? c : null,
				_ => null
			};
			
			if (connection == null)
				throw new LinqToDBException("Dataconnection is not iDB2Connection.");

			return connection;
		}

		public static BulkCopyType GetEffectiveType(this BulkCopyType bulkCopyType)
			=> bulkCopyType == BulkCopyType.Default ? DB2iSeriesTools.DefaultBulkCopyType : bulkCopyType;

		public static string GetLibList(this DataConnection dataConnection)
		{
			IEnumerable<string> libraries = new string[] { };
			var connection = GetProviderConnection(dataConnection);

			if (dataConnection.DataProvider is IDB2iSeriesDataProvider iSeriesDataProvider)
			{
				if (iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.AccessClient
					&& iSeriesDataProvider.Adapter is DB2iSeriesProviderAdapter accessClientAdapter)
				{
					libraries = accessClientAdapter.GetLibraryList(connection).Split(',');
				}
				else if (iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.Odbc)
				{
					var csb = new DbConnectionStringBuilder() { ConnectionString = dataConnection.ConnectionString };
					if (csb.TryGetValue("DBQ", out var librariesString))
					{
						libraries = librariesString.ToString().Split(' ').Select(x => x.Trim());
					}
				}
				else if (iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.OleDb)
				{
					var csb = new DbConnectionStringBuilder() { ConnectionString = dataConnection.ConnectionString };
					if (csb.TryGetValue("Default Collection", out var librariesString))
					{
						libraries = librariesString.ToString().Split(',').Select(x => x.Trim());
					}
				}
				else if (iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.DB2)
				{
					var csb = new DbConnectionStringBuilder() { ConnectionString = dataConnection.ConnectionString };
					if (csb.TryGetValue("Library List", out var librariesString))
					{
						libraries = librariesString.ToString().Split(' ').Select(x => x.Trim());
					}
				}
			}

			return string.Join("','", libraries);
		}

		public static string GetDelimiter(this DataConnection dataConnection)
		{
			var connection = GetProviderConnection(dataConnection);
			var namingConvention = DB2iSeriesNamingConvention.Sql;

			if (dataConnection.DataProvider is IDB2iSeriesDataProvider iSeriesDataProvider)
			{
				if (iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.AccessClient 
					&& iSeriesDataProvider.Adapter is DB2iSeriesProviderAdapter accessClientAdapter)
				{
					namingConvention = accessClientAdapter.GetNamingConvention(connection) == DB2iSeriesProviderAdapter.iDB2NamingConvention.SQL ? 
						DB2iSeriesNamingConvention.Sql : DB2iSeriesNamingConvention.System;
				}
				else if (iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.Odbc
					|| iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.OleDb)
				{
					var csb = new DbConnectionStringBuilder() { ConnectionString = dataConnection.ConnectionString };
					
					var propertyName = iSeriesDataProvider.ProviderType == DB2iSeriesAdoProviderType.Odbc ? "NAM" : "Naming Convention";

					if (csb.TryGetValue(propertyName, out var naming))
					{
						namingConvention = naming.ToString() == "1" ? DB2iSeriesNamingConvention.System : DB2iSeriesNamingConvention.Sql;
					}
				}
			}

			return Constants.SQL.Delimiter(namingConvention);
		}
	}
}
