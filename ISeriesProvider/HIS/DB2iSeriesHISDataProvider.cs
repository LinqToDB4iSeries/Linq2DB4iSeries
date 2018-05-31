using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Extensions;
	
	public class DB2iSeriesHISDataProvider : DB2iSeriesDataProvider
	{
        public DB2iSeriesHISDataProvider() : this(DB2iSeriesHISProviderName.DB2, DB2iSeriesLevels.Any, false)
        {
        }

        public DB2iSeriesHISDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString) : base(name, minLevel, mapGuidAsString)
        {
            
        }
       
		#region "overrides"

		public override string ConnectionNamespace { get { return ""; } }
		protected override string ConnectionTypeName { get { return DB2iSeriesHISTools.ConnectionTypeName; } }
		protected override string DataReaderTypeName { get { return DB2iSeriesHISTools.DataReaderTypeName; } }
		
		protected override void OnConnectionTypeCreated(Type connectionType)
		{
            DB2iSeriesHISTypes.ConnectionType = connectionType;
            
            if (DataConnection.TraceSwitch.TraceInfo)
            {
                DataConnection.WriteTraceLine(
                    DataReaderType.AssemblyEx().FullName,
                    DataConnection.TraceSwitch.DisplayName);

                DataConnection.WriteTraceLine(
                    DB2iSeriesHISTypes.DB2DateTime.IsSupported ? "DB2DateTime is supported." : "DB2DateTime is not supported.",
                    DataConnection.TraceSwitch.DisplayName);
            }
            DB2iSeriesHISTools.Initialized();
		}

        protected override void SetParameterType(IDbDataParameter parameter, DataType dataType)
        {
            if (parameter is BulkCopyReader.Parameter)
                return;

            switch (dataType)
            {
                case DataType.SByte: parameter.DbType = DbType.Int16; break;
                case DataType.UInt16: parameter.DbType = DbType.Int32; break;
                case DataType.UInt32: parameter.DbType = DbType.Int64; break;
                case DataType.UInt64: parameter.DbType = DbType.Decimal; break;
                case DataType.VarNumeric: parameter.DbType = DbType.Decimal; break;
                case DataType.DateTime2: parameter.DbType = DbType.DateTime2; break;
                case DataType.Text: SetParameterDbType(parameter, MsDb2Type.VarChar); break;
                case DataType.NText: SetParameterDbType(parameter, MsDb2Type.NVarChar); break;
                case DataType.Binary: SetParameterDbType(parameter, MsDb2Type.Binary); break;
                case DataType.Blob: SetParameterDbType(parameter, MsDb2Type.BLOB); break;
                case DataType.VarBinary: SetParameterDbType(parameter, MsDb2Type.VarBinary); break;
                case DataType.Image: SetParameterDbType(parameter, MsDb2Type.Graphic); break;
                case DataType.Money: SetParameterDbType(parameter, MsDb2Type.Decimal); break;
                case DataType.SmallMoney: SetParameterDbType(parameter, MsDb2Type.Decimal); break;
                case DataType.Date: SetParameterDbType(parameter, MsDb2Type.Date); break;
                case DataType.Time: SetParameterDbType(parameter, MsDb2Type.Time); break;
                case DataType.SmallDateTime: SetParameterDbType(parameter, MsDb2Type.Timestamp); break;
                case DataType.Timestamp: SetParameterDbType(parameter, MsDb2Type.Timestamp); break;
                case DataType.Xml: SetParameterDbType(parameter, MsDb2Type.Xml); break;
                default: base.SetParameterType(parameter, dataType); break;
            }
        }

        private static Dictionary<T, object> MapEnum<T>(Type toEnumType)
            where T : struct
        {
            //var type = Type.GetType(DB2iSeriesHISTools.DbTypeTypeName);
            var underlyingType = toEnumType.GetEnumUnderlyingType();

            var enumValues = Enum.GetValues(toEnumType);
            var localValues = Enum.GetValues(typeof(T)).Cast<T>().ToDictionary(x => Convert.ChangeType(x, underlyingType));

            var dic = new Dictionary<T, object>();

            foreach (var item in enumValues)
            {
                if (localValues.TryGetValue(Convert.ChangeType(item, underlyingType), out var localItem))
                    dic.Add(localItem, item);
            }

            return dic;
        }

        private static void SetParameterDbType(IDbDataParameter parameter, MsDb2Type dbType)
        {
            parameterDbTypeSetter.Value.Invoke(parameter, enumMap.Value[dbType]);
        }

        private static Lazy<Dictionary<MsDb2Type, object>> enumMap = new Lazy<Dictionary<MsDb2Type, object>>(() =>
        {
            return MapEnum<MsDb2Type>(Type.GetType(DB2iSeriesHISTools.DbTypeTypeName));
        });

        private static Lazy<Action<IDbDataParameter, object>> parameterDbTypeSetter = new Lazy<Action<IDbDataParameter, object>>(() =>
        {
            var dbParameterType = Type.GetType(DB2iSeriesHISTools.DbTypeTypeName);
            var dbTypeType = Type.GetType(DB2iSeriesHISTools.DbTypeTypeName);

            var instanceParam = Expression.Parameter(typeof(IDbDataParameter));
            var valueParam = Expression.Parameter(typeof(object));
            var convertedInstanceParam = Expression.Convert(instanceParam, dbParameterType);
            var convertedValueParam = Expression.Convert(valueParam, dbTypeType);

            var setter = Expression.Lambda<Action<IDbDataParameter, object>>(
                Expression.Assign(
                    Expression.Property(convertedInstanceParam, "MsDb2Type"),
                    convertedValueParam));

            return setter.Compile();
        });

        private enum MsDb2Type : ushort
        {
            BigInt = 0,
            Binary = 1,
            Bit = 2,
            BLOB = 3,
            Boolean = 4,
            Char = 5,
            CharForBit = 6,
            CLOB = 7,
            Date = 8,
            DBCLOB = 9,
            DecFloat = 10,
            Decimal = 11,
            Double = 12,
            Graphic = 13,
            Int = 14,
            LongVarChar = 15,
            LongVarCharForBit = 16,
            LongVarGraphic = 17,
            NChar = 18,
            NClob = 19,
            Numeric = 20,
            NVarChar = 21,
            Real = 22,
            RowId = 23,
            SmallInt = 24,
            Time = 25,
            Timestamp = 26,
            TimeStampOffset = 27,
            TinyInt = 28,
            VarBinary = 29,
            VarChar = 30,
            VarCharForBit = 31,
            VarGraphic = 32,
            VarWideChar = 33,
            VarWideGraphic = 34,
            WideChar = 35,
            Xml = 36
        }
        #endregion
    }
}