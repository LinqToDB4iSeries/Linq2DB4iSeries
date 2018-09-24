using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using System.Linq.Expressions;
    using System.Reflection;
	using Extensions;
	using Mapping;
	using SqlQuery;

    public class DB2iSeriesMappingSchema : MappingSchema
    {
        readonly static Lazy<DB2iSeriesMappingSchema> instance = new Lazy<DB2iSeriesMappingSchema>(() => new DB2iSeriesMappingSchema());
        internal static DB2iSeriesMappingSchema Instance => instance.Value;
        public DB2iSeriesMappingSchema() : base(DB2iSeriesProviderName.DB2)
        {
            //if (mapGuidAsString)
            //  SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => ConvertGuidToSql(sb, (Guid)v));

            ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

            SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));
            SetValueToSqlConverter(typeof(string), (sb, dt, v) => ConvertStringToSql(sb, v.ToString()));
            SetValueToSqlConverter(typeof(char), (sb, dt, v) => ConvertCharToSql(sb, (char)v));
            SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => ConvertDateTimeToSql(sb, dt, (DateTime)v));

            AddMetadataReader(new DB2iSeriesMetadataReader(DB2iSeriesProviderName.DB2));
#if !NETSTANDARD2_0
            AddMetadataReader(new DB2iSeriesAttributeReader());
#endif
        }

        internal static void MapGuidAsString(MappingSchema mappingSchema)
        {
            mappingSchema.SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => ConvertGuidToSql(sb, (Guid)v));
        }

        private static void AppendConversion(StringBuilder stringBuilder, int value)
        {
            stringBuilder.Append("varchar(").Append(value).Append(")");
        }

        private static void ConvertStringToSql(StringBuilder stringBuilder, string value)
        {
            DataTools.ConvertStringToSql(stringBuilder, "||", "", AppendConversion, value, null);
        }

        private static void ConvertCharToSql(StringBuilder stringBuilder, char value)
        {
            DataTools.ConvertCharToSql(stringBuilder, "'", AppendConversion, value);
        }

        private static void ConvertDateTimeToSql(StringBuilder stringBuilder, SqlDataType datatype, DateTime value)
        {
            var format = value.Millisecond == 0 ?
                        "'{0:yyyy-MM-dd HH:mm:ss}'" :
                        "'{0:yyyy-MM-dd HH:mm:ss.fff}'";

            if (datatype.DataType == DataType.Date)
                format = "'{0:yyyy-MM-dd}'";

            if (datatype.DataType == DataType.Time)
            {
                format = value.Millisecond == 0 ?
                            "'{0:HH:mm:ss}'" :
                            "'{0:HH:mm:ss.fff}'";
            }

            stringBuilder.AppendFormat(format, value);
        }

        private static void ConvertGuidToSql(StringBuilder stringBuilder, Guid value)
        {
            dynamic s = value.ToString("N");
            stringBuilder
              .Append("Cast(x'")
              .Append(s.Substring(6, 2))
              .Append(s.Substring(4, 2))
              .Append(s.Substring(2, 2))
              .Append(s.Substring(0, 2))
              .Append(s.Substring(10, 2))
              .Append(s.Substring(8, 2))
              .Append(s.Substring(14, 2))
              .Append(s.Substring(12, 2))
              .Append(s.Substring(16, 16))
              .Append("' as char(16) for bit data)");
        }
    }

    public class DB2iSeriesAccessClientMappingSchema : MappingSchema
    {
        readonly static Lazy<DB2iSeriesAccessClientMappingSchema> instance = new Lazy<DB2iSeriesAccessClientMappingSchema>(() => new DB2iSeriesAccessClientMappingSchema());
        internal static DB2iSeriesAccessClientMappingSchema Instance => instance.Value;

        public DB2iSeriesAccessClientMappingSchema()
            : base(DB2iSeriesProviderName.DB2iSeries_AccessClient, DB2iSeriesMappingSchema.Instance)
        {
            BuildAccessClientMappings();
        }

        private static object GetNullValue(Type type)
        {
            dynamic getValue = Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, type, "Null"), typeof(object)));
            return getValue.Compile()();
        }

        protected void BuildAccessClientMappings()
        {
            AddScalarType(DB2iSeriesTypes.BigInt, GetNullValue(DB2iSeriesTypes.BigInt), true, DataType.Int64);
            AddScalarType(DB2iSeriesTypes.Binary, GetNullValue(DB2iSeriesTypes.Binary), true, DataType.Binary);
            AddScalarType(DB2iSeriesTypes.Blob, GetNullValue(DB2iSeriesTypes.Blob), true, DataType.Blob);
            AddScalarType(DB2iSeriesTypes.Char, GetNullValue(DB2iSeriesTypes.Char), true, DataType.Char);
            AddScalarType(DB2iSeriesTypes.CharBitData, GetNullValue(DB2iSeriesTypes.CharBitData), true, DataType.Binary);
            AddScalarType(DB2iSeriesTypes.Clob, GetNullValue(DB2iSeriesTypes.Clob), true, DataType.NText);
            AddScalarType(DB2iSeriesTypes.DataLink, GetNullValue(DB2iSeriesTypes.DataLink), true, DataType.NText);
            AddScalarType(DB2iSeriesTypes.Date, GetNullValue(DB2iSeriesTypes.Date), true, DataType.Date);
            AddScalarType(DB2iSeriesTypes.DbClob, GetNullValue(DB2iSeriesTypes.DbClob), true, DataType.NText);
            AddScalarType(DB2iSeriesTypes.DecFloat16, GetNullValue(DB2iSeriesTypes.DecFloat16), true, DataType.Decimal);
            AddScalarType(DB2iSeriesTypes.DecFloat34, GetNullValue(DB2iSeriesTypes.DecFloat34), true, DataType.Decimal);
            AddScalarType(DB2iSeriesTypes.Decimal, GetNullValue(DB2iSeriesTypes.Decimal), true, DataType.Decimal);
            AddScalarType(DB2iSeriesTypes.Double, GetNullValue(DB2iSeriesTypes.Double), true, DataType.Double);
            AddScalarType(DB2iSeriesTypes.Graphic, GetNullValue(DB2iSeriesTypes.Graphic), true, DataType.NText);
            AddScalarType(DB2iSeriesTypes.Integer, GetNullValue(DB2iSeriesTypes.Integer), true, DataType.Int32);
            AddScalarType(DB2iSeriesTypes.Numeric, GetNullValue(DB2iSeriesTypes.Numeric), true, DataType.Decimal);
            AddScalarType(DB2iSeriesTypes.Real, GetNullValue(DB2iSeriesTypes.Real), true, DataType.Single);
            AddScalarType(DB2iSeriesTypes.RowId, GetNullValue(DB2iSeriesTypes.RowId), true, DataType.VarBinary);
            AddScalarType(DB2iSeriesTypes.SmallInt, GetNullValue(DB2iSeriesTypes.SmallInt), true, DataType.Int16);
            AddScalarType(DB2iSeriesTypes.Time, GetNullValue(DB2iSeriesTypes.Time), true, DataType.Time);
            AddScalarType(DB2iSeriesTypes.TimeStamp, GetNullValue(DB2iSeriesTypes.TimeStamp), true, DataType.DateTime2);
            AddScalarType(DB2iSeriesTypes.VarBinary, GetNullValue(DB2iSeriesTypes.VarBinary), true, DataType.VarBinary);
            AddScalarType(DB2iSeriesTypes.VarChar, GetNullValue(DB2iSeriesTypes.VarChar), true, DataType.VarChar);
            AddScalarType(DB2iSeriesTypes.VarCharBitData, GetNullValue(DB2iSeriesTypes.VarCharBitData), true, DataType.VarBinary);
            AddScalarType(DB2iSeriesTypes.VarGraphic, GetNullValue(DB2iSeriesTypes.VarGraphic), true, DataType.NText);
            AddScalarType(DB2iSeriesTypes.Xml, GetNullValue(DB2iSeriesTypes.Xml), true, DataType.Xml);
        }
    }

    public class DB2iSeriesDB2ConnectMappingSchema : MappingSchema
    {
        readonly static Lazy<DB2iSeriesDB2ConnectMappingSchema> instance = new Lazy<DB2iSeriesDB2ConnectMappingSchema>(() => new DB2iSeriesDB2ConnectMappingSchema());
        internal static DB2iSeriesDB2ConnectMappingSchema Instance => instance.Value;

        public DB2iSeriesDB2ConnectMappingSchema()
            : base(DB2iSeriesProviderName.DB2iSeries_DB2Connect, DB2iSeriesMappingSchema.Instance)
        {
            BuildDB2ConnectMappings();
        }

        private static object GetNullValue(Type type)
        {
            dynamic getValue = Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, type, "Null"), typeof(object)));
            return getValue.Compile()();
        }

        protected void BuildDB2ConnectMappings()
        {
            AddScalarType(DB2Types.Instance.DB2Int64, GetNullValue(DB2Types.Instance.DB2Int64), true, DataType.Int64);
            AddScalarType(DB2Types.Instance.DB2Int32, GetNullValue(DB2Types.Instance.DB2Int32), true, DataType.Int32);
            AddScalarType(DB2Types.Instance.DB2Int16, GetNullValue(DB2Types.Instance.DB2Int16), true, DataType.Int16);
            AddScalarType(DB2Types.Instance.DB2Decimal, GetNullValue(DB2Types.Instance.DB2Decimal), true, DataType.Decimal);
            AddScalarType(DB2Types.Instance.DB2DecimalFloat, GetNullValue(DB2Types.Instance.DB2DecimalFloat), true, DataType.Decimal);
            AddScalarType(DB2Types.Instance.DB2Real, GetNullValue(DB2Types.Instance.DB2Real), true, DataType.Single);
            AddScalarType(DB2Types.Instance.DB2Real370, GetNullValue(DB2Types.Instance.DB2Real370), true, DataType.Single);
            AddScalarType(DB2Types.Instance.DB2Double, GetNullValue(DB2Types.Instance.DB2Double), true, DataType.Double);
            AddScalarType(DB2Types.Instance.DB2String, GetNullValue(DB2Types.Instance.DB2String), true, DataType.NVarChar);
            AddScalarType(DB2Types.Instance.DB2Clob, GetNullValue(DB2Types.Instance.DB2Clob), true, DataType.NText);
            AddScalarType(DB2Types.Instance.DB2Binary, GetNullValue(DB2Types.Instance.DB2Binary), true, DataType.VarBinary);
            AddScalarType(DB2Types.Instance.DB2Blob, GetNullValue(DB2Types.Instance.DB2Blob), true, DataType.Blob);
            AddScalarType(DB2Types.Instance.DB2Date, GetNullValue(DB2Types.Instance.DB2Date), true, DataType.Date);
            AddScalarType(DB2Types.Instance.DB2Time, GetNullValue(DB2Types.Instance.DB2Time), true, DataType.Time);
            AddScalarType(DB2Types.Instance.DB2TimeStamp, GetNullValue(DB2Types.Instance.DB2TimeStamp), true, DataType.DateTime2);
            AddScalarType(DB2Types.Instance.DB2RowId, GetNullValue(DB2Types.Instance.DB2RowId), true, DataType.VarBinary);
            AddScalarType(DB2Types.Instance.DB2Xml, DB2.DB2Tools.IsCore ? null : GetNullValue(DB2Types.Instance.DB2Xml), true, DataType.Xml);
        }
    }

    public class DB2iSeriesAccessClientMappingSchema_GAS : MappingSchema
    {
        readonly static Lazy<DB2iSeriesAccessClientMappingSchema_GAS> instance = new Lazy<DB2iSeriesAccessClientMappingSchema_GAS>(() => new DB2iSeriesAccessClientMappingSchema_GAS());
        internal static DB2iSeriesAccessClientMappingSchema_GAS Instance => instance.Value;
        public DB2iSeriesAccessClientMappingSchema_GAS()
            : base(DB2iSeriesProviderName.DB2iSeries_AccessClient_GAS, DB2iSeriesMappingSchema.Instance, DB2iSeriesAccessClientMappingSchema.Instance)
        {
            DB2iSeriesMappingSchema.MapGuidAsString(this);
        }
    }

    public class DB2iSeriesDB2ConnectMappingSchema_GAS : MappingSchema
    {
        readonly static Lazy<DB2iSeriesDB2ConnectMappingSchema_GAS> instance = new Lazy<DB2iSeriesDB2ConnectMappingSchema_GAS>(() => new DB2iSeriesDB2ConnectMappingSchema_GAS());
        internal static DB2iSeriesDB2ConnectMappingSchema_GAS Instance => instance.Value;
        public DB2iSeriesDB2ConnectMappingSchema_GAS()
            : base(DB2iSeriesProviderName.DB2iSeries_DB2Connect_GAS, DB2iSeriesMappingSchema.Instance, DB2iSeriesDB2ConnectMappingSchema.Instance)
        {
            DB2iSeriesMappingSchema.MapGuidAsString(this);
        }
    }
}
