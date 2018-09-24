using System;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using Data;
    using Configuration;
    using Extensions;
    using Reflection;
    using System.Linq;
    using System.Linq.Expressions;

    public static class DB2iSeriesTypes
    {
        // https://secure.pamtransport.com/bin/IBM.Data.DB2.iSeries.xml

        public static readonly TypeCreator<long> BigInt = new TypeCreator<long>();
        public static readonly TypeCreator<byte[]> Binary = new TypeCreator<byte[]>();
        public static readonly DB2iSeriesTypeCreator<byte[]> Blob = new DB2iSeriesTypeCreator<byte[]>();
        public static readonly TypeCreator<string> Char = new TypeCreator<string>();
        public static readonly TypeCreator<byte[]> CharBitData = new TypeCreator<byte[]>();
        public static readonly DB2iSeriesTypeCreator<string> Clob = new DB2iSeriesTypeCreator<string>();
        public static readonly TypeCreator<string> DataLink = new TypeCreator<string>();
        public static readonly TypeCreator<DateTime> Date = new TypeCreator<DateTime>();
        public static readonly DB2iSeriesTypeCreator<string> DbClob = new DB2iSeriesTypeCreator<string>();
        public static readonly TypeCreator<decimal, double, long> DecFloat16 = new TypeCreator<decimal, double, long>();
        public static readonly TypeCreator<decimal, double, long> DecFloat34 = new TypeCreator<decimal, double, long>();
        public static readonly TypeCreator<decimal> Decimal = new TypeCreator<decimal>();
        public static readonly TypeCreator<double> Double = new TypeCreator<double>();
        public static readonly TypeCreator<string> Graphic = new TypeCreator<string>();
        public static readonly TypeCreator<int> Integer = new TypeCreator<int>();
        public static readonly TypeCreator<decimal> Numeric = new TypeCreator<decimal>();
        public static readonly TypeCreator<float> Real = new TypeCreator<float>();
        public static readonly TypeCreator<byte[]> RowId = new TypeCreator<byte[]>();
        public static readonly TypeCreator<short> SmallInt = new TypeCreator<short>();
        public static readonly TypeCreator<DateTime> Time = new TypeCreator<DateTime>();
        public static readonly TypeCreator<DateTime> TimeStamp = new TypeCreator<DateTime>();
        public static readonly TypeCreator<byte[]> VarBinary = new TypeCreator<byte[]>();
        public static readonly TypeCreator<string> VarChar = new TypeCreator<string>();
        public static readonly TypeCreator<byte[]> VarCharBitData = new TypeCreator<byte[]>();
        public static readonly TypeCreator<string> VarGraphic = new TypeCreator<string>();

        public static readonly TypeCreator<string> Xml = new TypeCreator<string>();
        public static Type ConnectionType { get; set; }

    }

    public class DB2TypeDescriptor
    {
        public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true)
        {
            DotnetType = dotnetType;
            DatareaderGetMethodName = datareaderGetMethodName;
            DatatypeName = datatypeName;
            DataType = dataType;
            CanBeNull = canBeNull;

            type = new Lazy<Type>(() => DB2Types.ConnectionType.Assembly.GetType(DatatypeName));
            nullValue = new Lazy<object>(() => isNullValueSet ? overridenNullValue : GetNullValue());
        }

        public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true)
            : this(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
            isNullValueSet = true;
            overridenNullValue = nullValue;
        }

        private readonly Lazy<Type> type;
        private readonly Lazy<object> nullValue;
        private readonly object overridenNullValue;
        private readonly bool isNullValueSet = false;

        public Type Type => type.Value;
        public object NullValue => nullValue.Value;

        public Type DotnetType { get; }
        public string DatareaderGetMethodName { get; }
        public string DatatypeName { get; }
        public DataType DataType { get; }
        public bool CanBeNull { get; }

        private object GetNullValue()
        {
            var field = Type.GetFieldEx("Null");
            if (field == null)
                return null;
            
            return Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, field), typeof(object))).Compile()();
        }

        public static implicit operator Type(DB2TypeDescriptor dB2TypeDescriptor) => dB2TypeDescriptor.Type;
    }

    public class DB2TypeDescriptorDefault : DB2TypeDescriptor
    {
        private readonly DB2.TypeCreator creator = new DB2.TypeCreator();

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
        private readonly DB2.TypeCreator<T> creator = new DB2.TypeCreator<T>();

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
        private readonly DB2.TypeCreator<T1, T2, T3> creator = new DB2.TypeCreator<T1, T2, T3>();

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
            return creator.CreateInstance(value);
        }
    }

    public class DbTypeMetadata
    {
        public DbTypeMetadata(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName)
        {
            DotnetType = dotnetType;
            DatareaderGetMethodName = datareaderGetMethodName;
            DatatypeName = datatypeName;
            DataType = dataType;
        }

        public Type DotnetType { get; }
        public string DatareaderGetMethodName { get; }
        public string DatatypeName { get; }
        public DataType DataType { get; }


    }

    public class DB2Types
    {
        //internal const string AssemblyName_DB2Connect_Net = "IBM.Data.DB2";
        //internal const string AssemblyName_DB2Connect_Core = "IBM.Data.DB2.Core";
        internal const string NamespaceNameDB2Types = "IBM.Data.DB2Types";

        internal static readonly string AssemblyName = DB2.DB2Tools.AssemblyName;
        internal static readonly string NamespaceName = AssemblyName;

        internal static readonly string ConnectionTypeName = NamespaceName + ".DB2Connection, " + AssemblyName;
        internal static readonly string DataReaderTypeName = NamespaceName + ".DB2DataReader, " + AssemblyName;

        internal static readonly string TypeNameDbConnectionStringBuilder = AssemblyName + ".DB2ConnectionStringBuilder, " + AssemblyName;

        private readonly static Lazy<Type> connectionType = new Lazy<Type>(() => Type.GetType(ConnectionTypeName, true));

        public static Type ConnectionType => connectionType.Value;

        public static DB2TypeDescriptor<long> DB2Int64 { get; } = new DB2TypeDescriptor<long>(DataType.Int64, "GetDB2Int64", "DB2Int64");
        public static DB2TypeDescriptor<int> DB2Int32 { get; } = new DB2TypeDescriptor<int>(DataType.Int32, "GetDB2Int32", "DB2Int32");
        public static DB2TypeDescriptor<short> DB2Int16 { get; } = new DB2TypeDescriptor<short>(DataType.Int16, "GetDB2Int16", "DB2Int16");
        public static DB2TypeDescriptor<decimal, double, long> DB2DecimalFloat { get; } = new DB2TypeDescriptor<decimal, double, long>(DataType.Decimal, "GetDB2DecimalFloat", "DB2Decimal");
        public static DB2TypeDescriptor<decimal> DB2Decimal { get; } = new DB2TypeDescriptor<decimal>(DataType.Decimal, "GetDB2Decimal", "DB2DecimalFloat");
        public static DB2TypeDescriptor<float> DB2Real { get; } = new DB2TypeDescriptor<float>(DataType.Single, "GetDB2Real", "DB2Real");
        public static DB2TypeDescriptor<float> DB2Real370 { get; } = new DB2TypeDescriptor<float>(DataType.Single, "GetDB2Real370", "DB2Real370");
        public static DB2TypeDescriptor<double> DB2Double { get; } = new DB2TypeDescriptor<double>(DataType.Double, "GetDB2Double", "DB2Double");
        public static DB2TypeDescriptor<string> DB2String { get; } = new DB2TypeDescriptor<string>(DataType.NVarChar, "GetDB2String", "DB2String");
        public static DB2TypeDescriptorConnectionCreator<string> DB2Clob { get; } = new DB2TypeDescriptorConnectionCreator<string>(DataType.NText, "GetDB2Clob", "DB2Clob");
        public static DB2TypeDescriptor<byte[]> DB2Binary { get; } = new DB2TypeDescriptor<byte[]>(DataType.VarBinary, "GetDB2Binary", "DB2Binary");
        public static DB2TypeDescriptorConnectionCreator<byte[]> DB2Blob { get; } = new DB2TypeDescriptorConnectionCreator<byte[]>(DataType.Blob, "GetDB2Blob", "DB2Blob");
        public static DB2TypeDescriptor<DateTime> DB2Date { get; } = new DB2TypeDescriptor<DateTime>(DataType.Date, "GetDB2Date", "DB2Date");
        public static DB2TypeDescriptor<TimeSpan> DB2Time { get; } = new DB2TypeDescriptor<TimeSpan>(DataType.Time, "GetDB2Time", "DB2Time");
        public static DB2TypeDescriptor<DateTime> DB2TimeStamp { get; } = new DB2TypeDescriptor<DateTime>(DataType.DateTime2, "GetDB2TimeStamp", "DB2TimeStamp");
        public static DB2TypeDescriptorDefault DB2RowId { get; } = new DB2TypeDescriptorDefault(typeof(byte[]), DataType.VarBinary, "GetDB2RowId", "DB2RowId");
        public static DB2TypeDescriptor DB2Xml { get; } = new DB2TypeDescriptor(typeof(string), DataType.Xml, "GetDB2Xml", "DB2Xml");

        public static DB2TypeDescriptor[] AllTypes { get; }
                =
                typeof(DB2Types)
                    .GetPropertiesEx()
                    .Where(x => x.PropertyType == typeof(DB2TypeDescriptor))
                    .Select(x => x.GetValue(null).ToString())
                    .Cast<DB2TypeDescriptor>()
                    .ToArray();

        
        //private static Assembly AutoLoadAssembly()
        //{
        //    return Assembly.LoadFile(System.IO.Path.GetDirectoryName(Assembly.GetCallingAssembly().Location) + "\\" + AssemblyName + ".dll");
        //}
    }
}