using System;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using LinqToDB.Common;
	using Mapping;
	using SqlQuery;
	using System.Data.Linq;
	using static DB2iSeriesSqlBuilder;

	internal sealed class DB2iSeriesMappingSchemaBase : LockedMappingSchema
	{
		public static DB2iSeriesMappingSchemaBase Instance { get; } = new();

		DB2iSeriesMappingSchemaBase() 
			: base(DB2iSeriesProviderName.DB2)
		{
			ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

			SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));

			SetValueToSqlConverter(typeof(string), (sb, _, v) => ConvertStringToSql(sb, v.ToString()));
			SetValueToSqlConverter(typeof(char), (sb, _, v) => ConvertCharToSql(sb, (char)v));
			SetValueToSqlConverter(typeof(byte[]), (sb, _, v) => ConvertBinaryToSql(sb, (byte[])v));
			SetValueToSqlConverter(typeof(Binary), (sb, _, v) => ConvertBinaryToSql(sb, ((Binary)v).ToArray()));
			SetValueToSqlConverter(typeof(TimeSpan), (sb, _, v) => ConvertTimeToSql(sb, (TimeSpan)v));
			SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => ConvertDateTimeToSql(sb, dt.Type.DataType, (DateTime)v, precision: dt.Type.Precision));

			

			// set reader conversions from literals
			SetConverter<string, DateTime>(SqlDateTimeParser.ParseDateTime);

#if NET6_0_OR_GREATER
			SetValueToSqlConverter(typeof(DateOnly), (sb, _, _, v) => ConvertDateOnlyToSql(sb, (DateOnly)v));
			SetConverter<string, DateOnly>(ParseDateOnly);
#endif

			AddMetadataReader(new DB2iSeriesMetadataReader(DB2iSeriesProviderName.DB2));
		}
	}

	sealed class DB2iSeriesMappingSchema : LockedMappingSchema
	{
		public DB2iSeriesMappingSchema(string configuration, MappingSchema providerSchema) 
			: base(configuration, providerSchema, DB2iSeriesMappingSchemaBase.Instance)
		{
			SetValueToSqlConverter(typeof(Guid), (sb, _, _, v) => DB2iSeriesSqlBuilder.ConvertGuidToSql(sb, (Guid)v));
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
