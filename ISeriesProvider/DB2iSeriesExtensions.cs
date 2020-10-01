using LinqToDB.Common;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;
using System;

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
	}
}
