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
        public readonly static DB2TypeDescriptor<string> DB2Clob  = new DB2TypeDescriptor<string>(DataType.NText, "GetDB2Clob", "DB2Clob");
        public readonly static DB2TypeDescriptor<byte[]> DB2Binary  = new DB2TypeDescriptor<byte[]>(DataType.VarBinary, "GetDB2Binary", "DB2Binary");
        public readonly static DB2TypeDescriptor<byte[]> DB2Blob  = new DB2TypeDescriptor<byte[]>(DataType.Blob, "GetDB2Blob", "DB2Blob");
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

    #region DB2TypeDescriptor

    public class DB2TypeDescriptor : DB2iSeriesTypeDescriptorBase
    {
        public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2TypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
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

        public DB2TypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
            creator = new Lazy<Func<object>>(() => TypeCreatorBuilder.BuildTypeCreator(Type));
        }

        public DB2TypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
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

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
            creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
        }

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
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

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
            creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
        }

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
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

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
            creator = new Lazy<Func<T3, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T3>(Type));
        }

        public DB2TypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
            creator = new Lazy<Func<T3, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T3>(Type));
        }

        public object CreateInstance(T3 value)
        {
            return creator.Value(value);
        }
    }

    #endregion
}