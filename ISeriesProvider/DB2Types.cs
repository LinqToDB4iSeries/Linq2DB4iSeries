using System;
using System.Data.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using Extensions;
    using LinqToDB.Data;
    using System.Linq;

    public static class DB2Types
    {
        internal const string AssemblyName_Net = "IBM.Data.DB2";
        internal const string AssemblyName_Core = "IBM.Data.DB2.Core";
        
        internal static readonly string AssemblyName = DB2.DB2Tools.AssemblyName;
        internal static readonly string NamespaceName = AssemblyName;
        internal static readonly string TypesNamespaceName = "IBM.Data.DB2Types";

        internal static readonly string ConnectionTypeName = NamespaceName + ".DB2Connection, " + AssemblyName;
        internal static readonly string DataReaderTypeName = NamespaceName + ".DB2DataReader, " + AssemblyName;

        internal static readonly string TypeNameDbConnectionStringBuilder = AssemblyName + ".DB2ConnectionStringBuilder, " + AssemblyName;

        private readonly static Lazy<Type> connectionType = new Lazy<Type>(() => Type.GetType(ConnectionTypeName, true), true);

        public static Type ConnectionType => connectionType.Value;

        public readonly static DB2TypeDescriptor<long> DB2Int64  = new DB2TypeDescriptor<long>(DataType.Int64, "GetDB2Int64", "DB2Int64");
        public readonly static DB2TypeDescriptor<int> DB2Int32  = new DB2TypeDescriptor<int>(DataType.Int32, "GetDB2Int32", "DB2Int32");
        public readonly static DB2TypeDescriptor<short> DB2Int16  = new DB2TypeDescriptor<short>(DataType.Int16, "GetDB2Int16", "DB2Int16");
        public readonly static DB2TypeDescriptor<decimal, double, long> DB2DecimalFloat  = new DB2TypeDescriptor<decimal, double, long>(DataType.Decimal, "GetDB2DecimalFloat", "DB2Decimal");
        public readonly static DB2TypeDescriptor<decimal> DB2Decimal  = new DB2TypeDescriptor<decimal>(DataType.Decimal, "GetDB2Decimal", "DB2DecimalFloat");
        public readonly static DB2TypeDescriptor<float> DB2Real  = new DB2TypeDescriptor<float>(DataType.Single, "GetDB2Real", "DB2Real");
        public readonly static DB2TypeDescriptor<float> DB2Real370  = new DB2TypeDescriptor<float>(DataType.Single, "GetDB2Real370", "DB2Real370");
        public readonly static DB2TypeDescriptor<double> DB2Double  = new DB2TypeDescriptor<double>(DataType.Double, "GetDB2Double", "DB2Double");
        public readonly static DB2TypeDescriptor<string> DB2String  = new DB2TypeDescriptor<string>(DataType.NVarChar, "GetDB2String", "DB2String");
        public readonly static DB2TypeDescriptorConnectionCreator<string> DB2Clob  = new DB2TypeDescriptorConnectionCreator<string>(DataType.NText, "GetDB2Clob", "DB2Clob");
        public readonly static DB2TypeDescriptor<byte[]> DB2Binary  = new DB2TypeDescriptor<byte[]>(DataType.VarBinary, "GetDB2Binary", "DB2Binary");
        public readonly static DB2TypeDescriptorConnectionCreator<byte[]> DB2Blob  = new DB2TypeDescriptorConnectionCreator<byte[]>(DataType.Blob, "GetDB2Blob", "DB2Blob");
        public readonly static DB2TypeDescriptor<DateTime> DB2Date  = new DB2TypeDescriptor<DateTime>(DataType.Date, "GetDB2Date", "DB2Date");
        public readonly static DB2TypeDescriptor<TimeSpan> DB2Time  = new DB2TypeDescriptor<TimeSpan>(DataType.Time, "GetDB2Time", "DB2Time");
        public readonly static DB2TypeDescriptor<DateTime> DB2TimeStamp  = new DB2TypeDescriptor<DateTime>(DataType.DateTime2, "GetDB2TimeStamp", "DB2TimeStamp");
        public readonly static DB2TypeDescriptorDefault DB2RowId  = new DB2TypeDescriptorDefault(typeof(byte[]), DataType.VarBinary, "GetDB2RowId", "DB2RowId");
        public readonly static DB2TypeDescriptor DB2Xml  = new DB2TypeDescriptor(typeof(string), DataType.Xml, "GetDB2Xml", "DB2Xml");

        public static DB2TypeDescriptor[] AllTypes { get; }
                =
                typeof(DB2Types)
                    .GetPropertiesEx()
                    .Where(x => x.PropertyType == typeof(DB2TypeDescriptor))
                    .Select(x => (DB2TypeDescriptor)x.GetValue(null))
                    .ToArray();

        private static readonly Lazy<StaticTypeCreator<string, DbConnectionStringBuilder>> dbConnectionStringBuilderCreator
            = new Lazy<StaticTypeCreator<string, DbConnectionStringBuilder>>(() =>
                new StaticTypeCreator<string, DbConnectionStringBuilder>(
                  Type.GetType(TypeNameDbConnectionStringBuilder, true)));

        public static DbConnectionStringBuilder CreateConnectionStringBuilder(string connectionString = null)
            => connectionString == null ?
            dbConnectionStringBuilderCreator.Value.CreateInstance() :
            dbConnectionStringBuilderCreator.Value.CreateInstance(connectionString);
    }

    #region Type Descriptors

    public class DB2TypeDescriptor : DB2iSeriesTypeDescriptorBase
    {
        public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
        }

        protected override Type GetDB2Type()
        {
            return Type.GetType($"{DB2Types.TypesNamespaceName}.{DatatypeName},{DB2Types.AssemblyName}");
        }
    }
    public class DB2TypeDescriptorDefault : DB2TypeDescriptor
    {
        private readonly TypeCreator creator = new TypeCreator();

        public DB2TypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2TypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
        }

        public object CreateInstance()
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance();
        }
    }
    public class DB2TypeDescriptor<T> : DB2TypeDescriptor
    {
        private readonly TypeCreator<T> creator = new TypeCreator<T>();

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
        }

        public object CreateInstance()
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance();
        }

        public object CreateInstance(T value)
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance(value);
        }
    }
    public class DB2TypeDescriptor<T1, T2, T3> : DB2TypeDescriptor
    {
        private readonly TypeCreator<T1, T2, T3> creator = new TypeCreator<T1, T2, T3>();

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(typeof(T1), dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(typeof(T1), dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
        }

        public object CreateInstance()
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance();
        }

        public object CreateInstance(T1 value)
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance(value);
        }

        public object CreateInstance(T2 value)
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance(value);
        }

        public object CreateInstance(T3 value)
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance(value);
        }
    }
    public class DB2TypeDescriptorConnectionCreator<T> : DB2TypeDescriptor
    {
        private readonly DB2.ConnectionTypeTypeCreator<T> creator = new DB2.ConnectionTypeTypeCreator<T>();

        public DB2TypeDescriptorConnectionCreator(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2TypeDescriptorConnectionCreator(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
        }

        public object CreateInstance(T value)
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance(value);
        }

        public object CreateInstance(DataConnection value)
        {
            creator.Type = creator.Type ?? Type;
            if (DB2.DB2Types.ConnectionType == null)
                typeof(DB2.DB2Types).GetPropertyEx(nameof(DB2.DB2Types.ConnectionType)).SetValue(null, DB2Types.ConnectionType);

            return creator.CreateInstance(value);
        }
    }

    #endregion
}