using System;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Mapping;
	using SqlQuery;
	using System.Data.Linq;

	public abstract class DB2iSeriesMappingSchemaBase : MappingSchema
	{
		public bool GuidMappedAsString { get; protected set; } = false;
		
		protected DB2iSeriesMappingSchemaBase(string configuration, params MappingSchema[] schemas) : base(configuration, schemas)
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

			AddMetadataReader(new DB2iSeriesMetadataReader(configuration));
#if NETFRAMEWORK
			AddMetadataReader(new DB2iSeriesAttributeReader());
#endif
		}
	}

	public class DB2iSeriesMappingSchema : DB2iSeriesMappingSchemaBase
	{
		public DB2iSeriesMappingSchema()
			: this(DB2iSeriesProviderName.DB2)
		{
		}

		public DB2iSeriesMappingSchema(params MappingSchema[] schemas) 
			: this(DB2iSeriesProviderName.DB2, schemas)
		{
		}

		public DB2iSeriesMappingSchema(string configuration, params MappingSchema[] schemas) 
			: base(configuration, schemas)
		{
			SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => DB2iSeriesSqlBuilder.ConvertGuidToSql(sb, (Guid)v));
		}
	}

	public class DB2iSeriesGuidAsStringMappingSchema : DB2iSeriesMappingSchemaBase
	{
		public DB2iSeriesGuidAsStringMappingSchema()
			: this(DB2iSeriesProviderName.DB2_GAS)
		{
		}

		public DB2iSeriesGuidAsStringMappingSchema(params MappingSchema[] schemas) 
			: this(DB2iSeriesProviderName.DB2_GAS, schemas)
		{
		}

		public DB2iSeriesGuidAsStringMappingSchema(string configuration, params MappingSchema[] schemas) 
			: base(configuration, schemas)
		{
			GuidMappedAsString = true;
		}
	}
}
