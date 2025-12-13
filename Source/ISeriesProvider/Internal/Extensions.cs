using LinqToDB.Data;
using LinqToDB.Internal.SqlProvider;
using LinqToDB.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class Extensions
	{
		public static string? ToSqlString(this DbDataType dbDataType)
		{
			return DB2iSeriesSqlBuilder.GetDbType(dbDataType.DbType, dbDataType.Length, dbDataType.Precision, dbDataType.Scale);
		}

		public static bool IsLatinLetterOrNumber(this char c)
			=> (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');

		public static DbDataType GetTypeOrUnderlyingTypeDataType(this MappingSchema mappingSchema, Type type)
		{
			var sqlDataType = mappingSchema.GetDataType(type);
			if (sqlDataType.Type.DataType == DataType.Undefined)
				sqlDataType = mappingSchema.GetUnderlyingDataType(type, out var _);

			return sqlDataType.Type.DataType == DataType.Undefined ? DbDataType.Undefined : sqlDataType.Type;
		}

		public static bool IsGuidMappedAsString(this MappingSchema mappingSchema)
		{
			return mappingSchema is DB2iSeriesGuidAsStringMappingSchema;
		}

		public static DbDataType SanitizeDbDataType(this MappingSchema mappingSchema, DbDataType dbDataType, DB2iSeriesSqlProviderFlags flags)
		{
			return DB2iSeriesDbTypes.SanitizeDbDataType(dbDataType, mappingSchema.IsGuidMappedAsString(), flags.SupportsNCharTypes);
		}

		public static DbDataType GetDbTypeForCast(this MappingSchema mappingSchema, DbDataType type, object? value, DB2iSeriesSqlProviderFlags flags)
		{
			return DB2iSeriesDbTypes.UpCastDbDataTypeToFit(type, value);
		}

		public static DbConnection GetDbConnection(this DataConnection dataConnection)
		{
			var dbConnection = dataConnection.TryGetDbConnection();
			if (dbConnection == null)
				throw ExceptionHelper.MisssingDbConnection();

			return dbConnection;
		}

		public static string? GetString(this DbDataReader dbDataReader, string fieldName)
		{
			var index = dbDataReader.GetOrdinal(fieldName);
			var value = dbDataReader.GetValue(index);
			if (value is null || value == DBNull.Value)
				return null;

			if (value is string str)
				return str;

			return dbDataReader.GetValue(index).ToString();
		}

		public static string? GetTrimmedString(this DbDataReader dbDataReader, string fieldName)
		{
			return dbDataReader.GetString(fieldName)?.TrimEnd();
		}

		public static int GetInt32(this DbDataReader dbDataReader, string fieldName)
		{
			return dbDataReader.GetInt32(dbDataReader.GetOrdinal(fieldName));
		}

		public static int? GetNullableInt32(this DbDataReader dbDataReader, string fieldName)
		{
			var index = dbDataReader.GetOrdinal(fieldName);
			var value = dbDataReader.GetValue(index);
			if (value is null || value == DBNull.Value)
				return null;

			return dbDataReader.GetInt32(index);
		}

		public static bool Any(this DbDataReader dbDataReader, Func<DbDataReader, int, bool> predicate)
		{
			for (var i = 0; i < dbDataReader.FieldCount; i++)
			{
				if (predicate(dbDataReader, i))
					return true;
			}

			return false;
		}

		public static void ForEach<T>(this IEnumerable<T> enumerable, Action<T> action)
		{
			foreach(var item in enumerable)
				action(item);
		}

		public static BulkCopyType GetEffectiveType(this BulkCopyType bulkCopyType)
			=> bulkCopyType == BulkCopyType.Default ? DB2iSeriesOptions.Default.BulkCopyType : bulkCopyType;

		public static string GetQuotedLibList(this DataConnection dataConnection)
			=> "'" + string.Join("','", dataConnection.GetLibList()) + "'";

		public static IEnumerable<string> GetLibList(this DataConnection dataConnection)
		{
			var connection = dataConnection.TryGetDbConnection()
				?? throw ExceptionHelper.MisssingDbConnection();

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

				if (csb.TryGetValue(libraryListKey, out var libraryList)
					&& libraryList is not null)
				{
					return libraryList
						.ToString()!
						.Split(',', ' ')
						.Select(x => x.Trim())
						.Where(x => x != string.Empty)
						.ToList();
				}
				else
				{
					var lib = dataConnection.GetDefaultLib();
					if (lib != null)
					{
						return [lib];
					}
				}
			}

			return [];
		}

		public static string? GetDefaultLib(this DataConnection dataConnection)
		{
			var connection = dataConnection.TryGetDbConnection()
				?? throw ExceptionHelper.MisssingDbConnection();

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

				if (csb.TryGetValue(defaultLibKey, out var library)
					&& library is not null)
				{
					string result = library.ToString()!.Trim();
					if (!string.IsNullOrEmpty(result))
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
						namingConventionString = ((int)namingConvention).ToString(CultureInfo.InvariantCulture.NumberFormat);

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
			|| providerType == DB2iSeriesProviderType.OleDb;
	}
}
