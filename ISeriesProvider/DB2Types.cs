using System;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using Extensions;
    using LinqToDB.Common;
    using LinqToDB.Data;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Linq;

    public static class DB2Types
    {
        internal const string AssemblyName_Net = "IBM.Data.DB2";
        internal const string AssemblyName_Core = "IBM.Data.DB2.Core";

        internal static readonly bool IsCore = GetIsCore();
        internal static readonly string AssemblyName = IsCore ? AssemblyName_Core : AssemblyName_Net;
        internal static readonly string NamespaceName = AssemblyName;
        internal static readonly string TypesNamespaceName = "IBM.Data.DB2Types";

        internal static readonly string ConnectionTypeName = NamespaceName + ".DB2Connection, " + AssemblyName;
        internal static readonly string DataReaderTypeName = NamespaceName + ".DB2DataReader, " + AssemblyName;

        internal static readonly string TypeNameDbConnectionStringBuilder = AssemblyName + ".DB2ConnectionStringBuilder, " + AssemblyName;

        private readonly static Lazy<Type> connectionType = new Lazy<Type>(() => Type.GetType(ConnectionTypeName, true), true);

        public static Type ConnectionType => connectionType.Value;

        public readonly static DB2TypeDescriptor<long> DB2Int64 = new DB2TypeDescriptor<long>(DataType.Int64, "GetDB2Int64", "DB2Int64", (int)DB2Type.BigInt);
        public readonly static DB2TypeDescriptor<int> DB2Int32 = new DB2TypeDescriptor<int>(DataType.Int32, "GetDB2Int32", "DB2Int32", (int)DB2Type.Integer);
        public readonly static DB2TypeDescriptor<short> DB2Int16 = new DB2TypeDescriptor<short>(DataType.Int16, "GetDB2Int16", "DB2Int16", (int)DB2Type.SmallInt);
        public readonly static DB2TypeDescriptor<decimal, double, long> DB2DecimalFloat = new DB2TypeDescriptor<decimal, double, long>(DataType.Decimal, "GetDB2DecimalFloat", "DB2Decimal", (int)DB2Type.Decimal);
        public readonly static DB2TypeDescriptor<decimal> DB2Decimal = new DB2TypeDescriptor<decimal>(DataType.Decimal, "GetDB2Decimal", "DB2DecimalFloat", (int)DB2Type.DecimalFloat);
        public readonly static DB2TypeDescriptor<float> DB2Real = new DB2TypeDescriptor<float>(DataType.Single, "GetDB2Real", "DB2Real", (int)DB2Type.Real);
        public readonly static DB2TypeDescriptor<float> DB2Real370 = new DB2TypeDescriptor<float>(DataType.Single, "GetDB2Real370", "DB2Real370", (int)DB2Type.Real370);
        public readonly static DB2TypeDescriptor<double> DB2Double = new DB2TypeDescriptor<double>(DataType.Double, "GetDB2Double", "DB2Double", (int)DB2Type.Double);
        public readonly static DB2TypeDescriptor<string> DB2String = new DB2TypeDescriptor<string>(DataType.NVarChar, "GetDB2String", "DB2String", (int)DB2Type.Text);
        public readonly static DB2TypeDescriptorConnectionCreator<string> DB2Clob = new DB2TypeDescriptorConnectionCreator<string>(DataType.NText, "GetDB2Clob", "DB2Clob", (int)DB2Type.Clob);
        public readonly static DB2TypeDescriptor<byte[]> DB2Binary = new DB2TypeDescriptor<byte[]>(DataType.VarBinary, "GetDB2Binary", "DB2Binary", (int)DB2Type.Binary);
        public readonly static DB2TypeDescriptorConnectionCreator<byte[]> DB2Blob = new DB2TypeDescriptorConnectionCreator<byte[]>(DataType.Blob, "GetDB2Blob", "DB2Blob", (int)DB2Type.Blob);
        public readonly static DB2TypeDescriptor<DateTime> DB2Date = new DB2TypeDescriptor<DateTime>(DataType.Date, "GetDB2Date", "DB2Date", (int)DB2Type.Date);
        public readonly static DB2TypeDescriptor<TimeSpan> DB2Time = new DB2TypeDescriptor<TimeSpan>(DataType.Time, "GetDB2Time", "DB2Time", (int)DB2Type.Time);
        public readonly static DB2TypeDescriptor<DateTime> DB2TimeStamp = new DB2TypeDescriptor<DateTime>(DataType.DateTime2, "GetDB2TimeStamp", "DB2TimeStamp", (int)DB2Type.Timestamp);
        public readonly static DB2TypeDescriptorDefault DB2RowId = new DB2TypeDescriptorDefault(typeof(byte[]), DataType.VarBinary, "GetDB2RowId", "DB2RowId", (int)DB2Type.RowId);
        public readonly static DB2TypeDescriptor DB2Xml = new DB2TypeDescriptor(typeof(string), DataType.Xml, "GetDB2Xml", "DB2Xml", (int)DB2Type.Xml, isSupported: !IsCore);

        public static DB2TypeDescriptor[] AllTypes { get; }
                =
                typeof(DB2Types)
                    .GetStaticMembersEx("*")
                    .OfType<System.Reflection.FieldInfo>()
                    .Where(x => typeof(DB2TypeDescriptor).IsAssignableFromEx(x.FieldType))
                    .Select(x => (DB2TypeDescriptor)x.GetValue(null))
                    .ToArray();

        public static DB2TypeDescriptor GetTypeDescriptor(Type type)
        {
            if (typesDictionary.Value.TryGetValue(type, out var typeDescriptor))
                return typeDescriptor;

            return null;
        }

        private static readonly Lazy<IReadOnlyDictionary<Type, DB2TypeDescriptor>> typesDictionary =
            new Lazy<IReadOnlyDictionary<Type, DB2TypeDescriptor>>(() => AllTypes.ToDictionary(x => x.Type));

        private static readonly Lazy<Func<string, DbConnectionStringBuilder>> dbConnectionStringBuilderCreator
          = new Lazy<Func<string, DbConnectionStringBuilder>>(() =>
              TypeCreatorBuilder.BuildStaticTypeCreator<string, DbConnectionStringBuilder>(
                actualType: Type.GetType(TypeNameDbConnectionStringBuilder, true)));

        public static DbConnectionStringBuilder CreateConnectionStringBuilder(string connectionString = null)
            => dbConnectionStringBuilderCreator.Value(connectionString ?? "");

        #region DB2TypeDescriptor

        public class DB2TypeDescriptor : DB2iSeriesTypeDescriptorBase
        {
            public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName,int providerParameterDbType,  bool canBeNull = true, bool isSupported = true)
                : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull, isSupported)
            {
            }

            public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true, bool isSupported = true)
                : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull, isSupported)
            {
            }


            protected override Type GetDB2Type()
            {
                return Type.GetType($"{DB2Types.TypesNamespaceName}.{DatatypeName},{DB2Types.AssemblyName}");
            }
        }

        public class DB2TypeDescriptorDefault : DB2TypeDescriptor
        {
            private readonly Lazy<Func<object>> creator;

            public DB2TypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true, bool isSupported = true)
                : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull, isSupported)
            {
                creator = new Lazy<Func<object>>(() => TypeCreatorBuilder.BuildTypeCreator(Type));
            }

            public DB2TypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true, bool isSupported = true)
                : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull, isSupported)
            {
                creator = new Lazy<Func<object>>(() => TypeCreatorBuilder.BuildTypeCreator(Type));
            }

            public object CreateInstance()
            {
                return creator.Value();
            }
        }

        public class DB2TypeDescriptor<T> : DB2TypeDescriptorDefault
        {
            private readonly Lazy<Func<T, object>> creator;

            public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true, bool isSupported = true)
                : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
            }

            public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true, bool isSupported = true)
                : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
            }

            public object CreateInstance(T value)
            {
                return creator.Value(value);
            }
        }

        public class DB2TypeDescriptor<T1, T> : DB2TypeDescriptor<T1>
        {
            private readonly Lazy<Func<T, object>> creator;

            public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true, bool isSupported = true)
                : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
            }

            public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true, bool isSupported = true)
                : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
            }

            public object CreateInstance(T value)
            {
                return creator.Value(value);
            }
        }

        public class DB2TypeDescriptor<T1, T2, T3> : DB2TypeDescriptor<T1, T2>
        {
            private readonly Lazy<Func<T3, object>> creator;

            public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true, bool isSupported = true)
                : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T3, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T3>(Type));
            }

            public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true, bool isSupported = true)
                : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T3, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T3>(Type));
            }

            public object CreateInstance(T3 value)
            {
                return creator.Value(value);
            }
        }

        public class DB2TypeDescriptorConnectionCreator<T> : DB2TypeDescriptor
        {
            private readonly Lazy<Func<T, object>> creator;
            private readonly Lazy<Func<IDbConnection, object>> creatorConnection;
            public DB2TypeDescriptorConnectionCreator(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true, bool isSupported = true)
                : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
                creatorConnection = new Lazy<Func<IDbConnection, object>>(() => TypeCreatorBuilder.BuildTypeCreator<IDbConnection>(Type, ConnectionType));
            }

            public DB2TypeDescriptorConnectionCreator(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true, bool isSupported = true)
                : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull, isSupported)
            {
                creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
                creatorConnection = new Lazy<Func<IDbConnection, object>>(() => TypeCreatorBuilder.BuildTypeCreator<IDbConnection>(Type, ConnectionType));
            }

            public object CreateInstance(T value)
            {
                return creator.Value(value);
            }

            public object CreateInstance(IDbConnection value)
            {
                return creatorConnection.Value(value);
            }

            public object CreateInstance(DataConnection value)
            {
                return creatorConnection.Value(Proxy.GetUnderlyingObject(value.Connection));
            }
        }

        #endregion

        #region Proxy and IsCore from LinqToDB.DataProvider.DB2
        
        //From https://github.com/linq2db/linq2db/blob/master/Source/LinqToDB/DataProvider/DB2/DB2Tools.cs
        private static bool GetIsCore()
        {
            
            try
            {
                var path = typeof(DB2Types).AssemblyEx().GetPath();
                return File.Exists(Path.Combine(path, $"{AssemblyName_Core}.dll"));
            }
            catch (Exception)
            {
                return false;
            }
        }

        //From https://github.com/linq2db/linq2db/blob/master/Source/LinqToDB/Configuration/IProxy.cs

        internal static class Proxy
        {
            private static readonly Lazy<System.Reflection.MethodInfo> getUnderlyingObjectMethod
                = new Lazy<System.Reflection.MethodInfo>(() => {
                    var proxyType = typeof(DataType).AssemblyEx().GetType("LinqToDB.Configuration.Proxy", false);

                    if (proxyType == null) return null;

                    return proxyType
                        .GetStaticMembersEx(nameof(GetUnderlyingObject))
                        .OfType<System.Reflection.MethodInfo>()
                        .FirstOrDefault();
                });

            public static T GetUnderlyingObject<T>(T obj)
            {
                return getUnderlyingObjectMethod.Value == null ? obj : 
                    (T)getUnderlyingObjectMethod.Value.MakeGenericMethod(typeof(T)).Invoke(null, new object[] { obj });
            }
        }

        #endregion

        #region DB2Type for DB2Parameter

        //Taken from IBM.Data.DB2 11.1

        private enum DB2Type
        {
            Invalid = 0,
            SmallInt = 1,
            Integer = 2,
            BigInt = 3,
            Real = 4,
            Double = 5,
            Float = 6,
            Decimal = 7,
            Numeric = 8,
            Date = 9,
            Time = 10,
            Timestamp = 11,
            Char = 12,
            VarChar = 13,
            LongVarChar = 14,
            Binary = 15,
            VarBinary = 16,
            LongVarBinary = 17,
            Graphic = 18,
            VarGraphic = 19,
            LongVarGraphic = 20,
            Clob = 21,
            Blob = 22,
            DbClob = 23,
            Datalink = 24,
            RowId = 25,
            Xml = 26,
            Real370 = 27,
            DecimalFloat = 28,
            DynArray = 29,
            BigSerial = 30,
            BinaryXml = 31,
            TimeStampWithTimeZone = 32,
            Cursor = 33,
            Serial = 34,
            Int8 = 35,
            Serial8 = 36,
            Money = 37,
            DateTime = 38,
            Text = 39,
            Byte = 40,
            Char1 = 1001,
            SmallFloat = 1002,
            Null = 1003,
            IntervalYearMonth = 1004,
            IntervalDayFraction = 1005,
            NChar = 1006,
            NVarChar = 1007,
            Set = 1008,
            MultiSet = 1009,
            List = 1010,
            Row = 1011,
            SQLUDTVar = 1012,
            SQLUDTFixed = 1013,
            SmartLobLocator = 1014,
            Boolean = 1015,
            Other = 1016
        }

        #endregion
    }

   
}