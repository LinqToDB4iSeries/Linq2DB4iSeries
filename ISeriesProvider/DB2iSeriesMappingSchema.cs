﻿using System;
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
            ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

            SetDataType(typeof(string), new SqlDataType(DataType.NVarChar, typeof(string), 255));
            SetValueToSqlConverter(typeof(string), (sb, dt, v) => ConvertStringToSql(sb, v.ToString()));
            SetValueToSqlConverter(typeof(char), (sb, dt, v) => ConvertCharToSql(sb, (char)v));
            SetValueToSqlConverter(typeof(DateTime), (sb, dt, v) => ConvertDateTimeToSql(sb, dt, (DateTime)v));
            SetValueToSqlConverter(typeof(TimeSpan), (sb, dt, v) => ConvertTimeSpanToSql(sb, (TimeSpan)v));

            AddMetadataReader(new DB2iSeriesMetadataReader(DB2iSeriesProviderName.DB2));
#if !NETSTANDARD2_0
            AddMetadataReader(new DB2iSeriesAttributeReader());
#endif
        }

        internal static void MapGuidAsChar16(MappingSchema mappingSchema)
        {
            mappingSchema.SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => ConvertGuidToSql(sb, (Guid)v));
        }

        internal static void MapGuidAsString(MappingSchema mappingSchema)
        {
            mappingSchema.SetValueToSqlConverter(typeof(Guid), (sb, dt, v) => sb.Append("'{v}'"));
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

        private static void ConvertTimeSpanToSql(StringBuilder stringBuilder, TimeSpan value)
        {
            var format = value.Milliseconds == 0 ?
                        "'{0:HH:mm:ss}'" :
                        "'{0:HH:mm:ss.fff}'";

            stringBuilder.Append(value.ToString(format));
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
            DB2iSeriesMappingSchema.MapGuidAsChar16(this);
            BuildAccessClientMappings();
        }

        protected void BuildAccessClientMappings()
        {
            foreach (var type in DB2Types.AllTypes.Where(t => t.IsSupported))
                AddScalarType(type.Type, type.NullValue, type.CanBeNull, type.DataType);
        }
    }

    public class DB2iSeriesDB2ConnectMappingSchema : MappingSchema
    {
        readonly static Lazy<DB2iSeriesDB2ConnectMappingSchema> instance = new Lazy<DB2iSeriesDB2ConnectMappingSchema>(() => new DB2iSeriesDB2ConnectMappingSchema());
        internal static DB2iSeriesDB2ConnectMappingSchema Instance => instance.Value;

        public DB2iSeriesDB2ConnectMappingSchema()
            : base(DB2iSeriesProviderName.DB2iSeries_DB2Connect, DB2iSeriesMappingSchema.Instance)
        {
            DB2iSeriesMappingSchema.MapGuidAsChar16(this);
            BuildDB2ConnectMappings();
        }

        protected void BuildDB2ConnectMappings()
        {
            foreach (var type in DB2Types.AllTypes.Where(t => t.IsSupported))
                AddScalarType(type.Type, type.NullValue, type.CanBeNull, type.DataType);
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
