using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Extensions;
	using Mapping;
	using SchemaProvider;
	using SqlProvider;

	public class DB2iSeriesDB2ConnectDataProvider : DB2iSeriesDataProvider
	{
        public DB2iSeriesDB2ConnectDataProvider() : this(DB2iSeriesDB2ConnectProviderName.DB2, DB2iSeriesLevels.Any, false)
        {
        }

        public DB2iSeriesDB2ConnectDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString) : base(name, minLevel, mapGuidAsString)
        {

        }
        

		#region "overrides"

		public override string ConnectionNamespace { get { return ""; } }
		protected override string ConnectionTypeName { get { return DB2iSeriesDB2ConnectTools.ConnectionTypeName; } }
		protected override string DataReaderTypeName { get { return DB2iSeriesDB2ConnectTools.DataReaderTypeName; } }
		
		
		protected override void OnConnectionTypeCreated(Type connectionType)
		{
            DB2iSeriesDB2ConnectTypes.ConnectionType = connectionType;

            DB2iSeriesDB2ConnectTypes.DB2Int64.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Int64", true);
            DB2iSeriesDB2ConnectTypes.DB2Int32.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Int32", true);
            DB2iSeriesDB2ConnectTypes.DB2Int16.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Int16", true);
            DB2iSeriesDB2ConnectTypes.DB2Decimal.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Decimal", true);
            DB2iSeriesDB2ConnectTypes.DB2DecimalFloat.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2DecimalFloat", true);
            DB2iSeriesDB2ConnectTypes.DB2Real.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Real", true);
            DB2iSeriesDB2ConnectTypes.DB2Real370.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Real370", true);
            DB2iSeriesDB2ConnectTypes.DB2Double.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Double", true);
            DB2iSeriesDB2ConnectTypes.DB2String.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2String", true);
            DB2iSeriesDB2ConnectTypes.DB2Clob.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Clob", true);
            DB2iSeriesDB2ConnectTypes.DB2Binary.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Binary", true);
            DB2iSeriesDB2ConnectTypes.DB2Blob.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Blob", true);
            DB2iSeriesDB2ConnectTypes.DB2Date.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Date", true);
            DB2iSeriesDB2ConnectTypes.DB2Time.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Time", true);
            DB2iSeriesDB2ConnectTypes.DB2TimeStamp.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2TimeStamp", true);
            DB2iSeriesDB2ConnectTypes.DB2Xml = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2Xml", true);
            DB2iSeriesDB2ConnectTypes.DB2RowId.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2RowId", true);
            DB2iSeriesDB2ConnectTypes.DB2DateTime.Type = connectionType.AssemblyEx().GetType("IBM.Data.DB2Types.DB2DateTime", false);

            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Int64, typeof(Int64), "GetDB2Int64");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Int32, typeof(Int32), "GetDB2Int32");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Int16, typeof(Int16), "GetDB2Int16");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Decimal, typeof(Decimal), "GetDB2Decimal");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2DecimalFloat, typeof(Decimal), "GetDB2DecimalFloat");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Real, typeof(Single), "GetDB2Real");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Real370, typeof(Single), "GetDB2Real370");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Double, typeof(Double), "GetDB2Double");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2String, typeof(String), "GetDB2String");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Clob, typeof(String), "GetDB2Clob");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Binary, typeof(byte[]), "GetDB2Binary");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Blob, typeof(byte[]), "GetDB2Blob");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Date, typeof(DateTime), "GetDB2Date");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Time, typeof(TimeSpan), "GetDB2Time");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2TimeStamp, typeof(DateTime), "GetDB2TimeStamp");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2Xml, typeof(string), "GetDB2Xml");
            SetProviderField(DB2iSeriesDB2ConnectTypes.DB2RowId, typeof(byte[]), "GetDB2RowId");

            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Int64, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Int64), true, DataType.Int64);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Int32, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Int32), true, DataType.Int32);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Int16, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Int16), true, DataType.Int16);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Decimal, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Decimal), true, DataType.Decimal);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2DecimalFloat, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2DecimalFloat), true, DataType.Decimal);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Real, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Real), true, DataType.Single);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Real370, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Real370), true, DataType.Single);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Double, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Double), true, DataType.Double);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2String, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2String), true, DataType.NVarChar);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Clob, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Clob), true, DataType.NText);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Binary, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Binary), true, DataType.VarBinary);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Blob, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Blob), true, DataType.Blob);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Date, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Date), true, DataType.Date);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Time, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Time), true, DataType.Time);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2TimeStamp, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2TimeStamp), true, DataType.DateTime2);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2RowId, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2RowId), true, DataType.VarBinary);
            MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2Xml, DB2iSeriesDB2ConnectTools.IsCore ? null : GetNullValue(DB2iSeriesDB2ConnectTypes.DB2Xml), true, DataType.Xml);

            //_setBlob = GetSetParameter(connectionType, "DB2Parameter", "DB2Type", "DB2Type", "Blob");

            if (DB2iSeriesDB2ConnectTypes.DB2DateTime.IsSupported)
            {
                SetProviderField(DB2iSeriesDB2ConnectTypes.DB2DateTime, typeof(DateTime), "GetDB2DateTime");
                MappingSchema.AddScalarType(DB2iSeriesDB2ConnectTypes.DB2DateTime, GetNullValue(DB2iSeriesDB2ConnectTypes.DB2DateTime), true, DataType.DateTime);
            }

            if (DataConnection.TraceSwitch.TraceInfo)
            {
                DataConnection.WriteTraceLine(
                    DataReaderType.AssemblyEx().FullName,
                    DataConnection.TraceSwitch.DisplayName);

                DataConnection.WriteTraceLine(
                    DB2iSeriesDB2ConnectTypes.DB2DateTime.IsSupported ? "DB2DateTime is supported." : "DB2DateTime is not supported.",
                    DataConnection.TraceSwitch.DisplayName);
            }

            DB2iSeriesDB2ConnectTools.Initialized();
		}

        private static object GetNullValue(Type type)
        {
            dynamic getValue = Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, type, "Null"), typeof(object)));
            return getValue.Compile()();
        }

        #endregion
    }
}