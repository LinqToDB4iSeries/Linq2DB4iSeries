using LinqToDB.Internal.Mapping;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;
using System;
using System.Data.Linq;
using System.IO;
using static LinqToDB.DataProvider.DB2iSeries.DB2iSeriesSqlBuilder;


namespace LinqToDB.DataProvider.DB2iSeries
{
	internal sealed class DB2iSeriesMappingSchemaBase : LockedMappingSchema
	{
		public static DB2iSeriesMappingSchemaBase Instance { get; } = new();

		DB2iSeriesMappingSchemaBase() 
			: base(DB2iSeriesProviderName.DB2)
		{
			ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

			SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));

			SetValueToSqlConverter(typeof(string), (sb, _, v) => ConvertStringToSql(sb, (string)v));
			SetValueToSqlConverter(typeof(char), (sb, _, v) => ConvertCharToSql(sb, (char)v));
			SetValueToSqlConverter(typeof(byte[]), (sb, _, v) => ConvertBinaryToSql(sb, (byte[])v));
			SetValueToSqlConverter(typeof(Binary), (sb, _, v) => ConvertBinaryToSql(sb, ((Binary)v).ToArray()));
			SetValueToSqlConverter(typeof(TimeSpan), (sb, _, v) => ConvertTimeToSql(sb, (TimeSpan)v));
			SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => ConvertDateTimeToSql(sb, dt.Type.DataType, (DateTime)v, precision: dt.Type.Precision));

			// Set reader conversions from literals
			SetConverter<string, DateTime>(SqlDateTimeParser.ParseDateTime);
			SetConverter<byte[], Stream>(b => new MemoryStream(b));
			// OleDb/Odbc will try to map string representation of numbers to numeric types and the default mapping schema will throw an exception
			// when the values have a decimal point, even if the target value is an integer. We need to provide a converter that can handle this case.
			SetConverter<string, byte>(SqlNumberParser.ParseByte);
			SetConverter<string, short>(SqlNumberParser.ParseInt16);
			SetConverter<string, ushort>(SqlNumberParser.ParseUInt16);
			SetConverter<string, int>(SqlNumberParser.ParseInt32);
			SetConverter<string, uint>(SqlNumberParser.ParseUInt32);
			SetConverter<string, long>(SqlNumberParser.ParseInt64);
			SetConverter<string, ulong>(SqlNumberParser.ParseUInt64);

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
			SetValueToSqlConverter(typeof(Guid), (sb, _, _, v) => ConvertGuidToSql(sb, (Guid)v));
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
