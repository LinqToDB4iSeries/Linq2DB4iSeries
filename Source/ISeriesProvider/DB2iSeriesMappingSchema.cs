using System;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Common;
	using Mapping;
	using SqlQuery;
	using System.Data.Linq;

	sealed class DB2iSeriesMappingSchemaBase : LockedMappingSchema
	{
		public static DB2iSeriesMappingSchemaBase Instance { get; } = new();

		DB2iSeriesMappingSchemaBase() 
			: base(DB2iSeriesProviderName.DB2)
		{
			ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

			SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));

			SetValueToSqlConverter(typeof(string), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertStringToSql(sb, v.ToString()));
			SetValueToSqlConverter(typeof(char), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertCharToSql(sb, (char)v));
			SetValueToSqlConverter(typeof(byte[]), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertBinaryToSql(sb, (byte[])v));
			SetValueToSqlConverter(typeof(Binary), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertBinaryToSql(sb, ((Binary)v).ToArray()));
			SetValueToSqlConverter(typeof(TimeSpan), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertTimeToSql(sb, (TimeSpan)v));
			SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertDateTimeToSql(sb, dt.Type.DataType, (DateTime)v, precision: dt.Type.Precision));
			
			// set reader conversions from literals
			SetConverter<string, DateTime>(SqlDateTimeParser.ParseDateTime);

			AddMetadataReader(new DB2iSeriesMetadataReader(DB2iSeriesProviderName.DB2));
		}
	}

	sealed class DB2iSeriesMappingSchema : LockedMappingSchema
	{
		public DB2iSeriesMappingSchema(string configuration, MappingSchema providerSchema) 
			: base(configuration, providerSchema, DB2iSeriesMappingSchemaBase.Instance)
		{
			SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertGuidToSql(sb, (Guid)v));
		}
	}

	sealed class DB2iSeriesGuidAsStringMappingSchema : LockedMappingSchema
	{
		public DB2iSeriesGuidAsStringMappingSchema(string configuration, MappingSchema providerSchema) 
			: base(configuration, providerSchema, DB2iSeriesMappingSchemaBase.Instance)
		{
		}
	}
}
