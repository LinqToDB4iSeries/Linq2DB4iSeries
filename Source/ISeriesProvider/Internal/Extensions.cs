using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;
using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;
using System.Data.Common;
using LinqToDB.SqlProvider;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class Extensions
	{
		public static string ToSqlString(this DbDataType dbDataType)
		{
			return DB2iSeriesSqlBuilder.GetDbType(dbDataType.DbType, dbDataType.Length, dbDataType.Precision, dbDataType.Scale);
		}

		public static bool IsLatinLetterOrNumber(this char c)
			=> (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');

		public static SqlDataType GetTypeOrUnderlyingTypeDataType(this MappingSchema mappingSchema, Type type)
		{
			var sqlDataType = mappingSchema.GetDataType(type);
			if (sqlDataType.Type.DataType == DataType.Undefined)
				sqlDataType = mappingSchema.GetUnderlyingDataType(type, out var _);

			return sqlDataType.Type.DataType == DataType.Undefined ? SqlDataType.Undefined : sqlDataType;
		}

		public static bool IsGuidMappedAsString(this MappingSchema mappingSchema)
		{
			return mappingSchema is DB2iSeriesGuidAsStringMappingSchema;
		}

		public static DbDataType GetDbDataType(this MappingSchema mappingSchema, Type systemType, DataType dataType, int? length, int? precision, int? scale, bool forceDefaultAttributes, bool supportsNCharTypes)
		{
			return DB2iSeriesDbTypes.GetDbDataType(systemType, dataType, length, precision, scale, mappingSchema.IsGuidMappedAsString(), forceDefaultAttributes, supportsNCharTypes);
		}

		public static DbDataType GetDbTypeForCast(this MappingSchema mappingSchema, DB2iSeriesSqlProviderFlags flags, SqlDataType type)
		{
			return DB2iSeriesDbTypes.GetDbTypeForCast(type, mappingSchema, flags);
		}

		public static IDbConnection GetProviderConnection(this DataConnection dataConnection)
		{
			if (dataConnection.DataProvider is not DB2iSeriesDataProvider iSeriesDataProvider)
				throw ExceptionHelper.InvalidProvider(dataConnection.DataProvider);

			if (iSeriesDataProvider.TryGetProviderConnection(dataConnection, out var connection))
				return connection;

			throw ExceptionHelper.InvalidDbConnectionType(dataConnection.Connection);
		}

		public static BulkCopyType GetEffectiveType(this BulkCopyType bulkCopyType)
			=> bulkCopyType == BulkCopyType.Default ? DB2iSeriesOptions.Default.BulkCopyType : bulkCopyType;

		public static string GetQuotedLibList(this DataConnection dataConnection)
			=> "'" + string.Join("','", dataConnection.GetLibList()) + "'";

		public static IEnumerable<string> GetLibList(this DataConnection dataConnection)
		{
			IEnumerable<string> libraries = new string[] { };
			var connection = GetProviderConnection(dataConnection);

			if (dataConnection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider)
			{
				var libraryListKey = iSeriesDataProvider.ProviderType switch
				{
#if NETFRAMEWORK
					DB2iSeriesProviderType.AccessClient => "Library List",
#endif
					DB2iSeriesProviderType.Odbc => "DBQ",
					DB2iSeriesProviderType.OleDb => "Library List",
					DB2iSeriesProviderType.DB2 => "LibraryList",
					_ => throw ExceptionHelper.InvalidAdoProvider(iSeriesDataProvider.ProviderType)
				};

				var csb = new DbConnectionStringBuilder() { ConnectionString = dataConnection.ConnectionString };

				if (csb.TryGetValue(libraryListKey, out var libraryList))
				{
					return libraryList
						.ToString()
						.Split(',', ' ')
						.Select(x => x.Trim())
						.Where(x => x != string.Empty)
						.ToList();
				}
				else
				{
					string lib = dataConnection.GetDefaultLib();
					if(lib != null)
					{
						return new string[] { lib };
					}
				}
			}

			return Enumerable.Empty<string>();
		}

		public static string GetDefaultLib(this DataConnection dataConnection)
		{
			var connection = GetProviderConnection(dataConnection);

			if (dataConnection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider)
			{
				var defaultLibKey = iSeriesDataProvider.ProviderType switch
				{
#if NETFRAMEWORK
					DB2iSeriesProviderType.AccessClient => "Default Collection",
#endif
					DB2iSeriesProviderType.Odbc => "DBQ",
					DB2iSeriesProviderType.OleDb => "Default Collection",
					DB2iSeriesProviderType.DB2 => "CurrentSchema",
					_ => throw ExceptionHelper.InvalidAdoProvider(iSeriesDataProvider.ProviderType)
				};

				var csb = new DbConnectionStringBuilder() { ConnectionString = dataConnection.ConnectionString };

				if (csb.TryGetValue(defaultLibKey, out var library))
				{
					string result = library.ToString().Trim();
					if(!string.IsNullOrEmpty(result))
					{
						return result;
					}
				}
			}

			return null;
		}

		public static string GetDelimiter(this DataConnection dataConnection)
			=> Constants.SQL.Delimiter(dataConnection.GetNamingConvention());

		public static DB2iSeriesNamingConvention GetNamingConvention(this DataConnection dataConnection)
		{
			if (dataConnection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider
				&& iSeriesDataProvider.ProviderType != DB2iSeriesProviderType.DB2)
			{
				var namingConventionKey = iSeriesDataProvider.ProviderType switch
				{
#if NETFRAMEWORK
					DB2iSeriesProviderType.AccessClient => "Naming",
#endif
					DB2iSeriesProviderType.Odbc => "NAM",
					DB2iSeriesProviderType.OleDb => "Naming Convention",
					_ => throw ExceptionHelper.InvalidAdoProvider(iSeriesDataProvider.ProviderType)
				};

				var csb = new DbConnectionStringBuilder() { ConnectionString = dataConnection.ConnectionString };

				if (csb.TryGetValue(namingConventionKey, out var namingConvention))
				{
					if (namingConvention is not string namingConventionString)
						namingConventionString = ((int)namingConvention).ToString();

					return namingConventionString == "1" ? DB2iSeriesNamingConvention.System : DB2iSeriesNamingConvention.Sql;
				}
			}

			return DB2iSeriesNamingConvention.Sql;
		}

		public static void SetFlag(this SqlProviderFlags sqlProviderFlags, string flag, bool isSet)
		{
			if (isSet && !sqlProviderFlags.CustomFlags.Contains(flag))
				sqlProviderFlags.CustomFlags.Add(flag);

			if (!isSet && sqlProviderFlags.CustomFlags.Contains(flag))
				sqlProviderFlags.CustomFlags.Remove(flag);
		}

		public static bool IsIBM(this DB2iSeriesProviderType providerType)
			=> providerType == DB2iSeriesProviderType.DB2
#if NETFRAMEWORK
			|| providerType == DB2iSeriesProviderType.AccessClient
#endif
			;

		public static bool IsDB2(this DB2iSeriesProviderType providerType)
			 => providerType == DB2iSeriesProviderType.DB2;

		public static bool IsAccessClient(this DB2iSeriesProviderType providerType)
			 =>
#if NETFRAMEWORK
			providerType == DB2iSeriesProviderType.AccessClient;
#else
			false;
#endif

		public static bool IsOdbc(this DB2iSeriesProviderType providerType)
			 => providerType == DB2iSeriesProviderType.Odbc;

		public static bool IsOleDb(this DB2iSeriesProviderType providerType)
			 => providerType == DB2iSeriesProviderType.OleDb;

		public static bool IsOdbcOrOleDb(this DB2iSeriesProviderType providerType)
			 => providerType == DB2iSeriesProviderType.Odbc
			||	providerType == DB2iSeriesProviderType.OleDb;
	}
}
