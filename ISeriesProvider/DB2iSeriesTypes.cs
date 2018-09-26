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

    public static class DB2iSeriesTypes
    {
        internal static readonly string AssemblyName = "IBM.Data.DB2.iSeries";
        internal static readonly string NamespaceName = AssemblyName;
        internal static readonly string TypesNamespaceName = AssemblyName;

        internal static readonly string ConnectionTypeName = NamespaceName + ".iDB2Connection, " + AssemblyName;
        internal static readonly string DataReaderTypeName = NamespaceName + ".iDB2DataReader, " + AssemblyName;

        internal static readonly string TypeNameDbConnectionStringBuilder = AssemblyName + ".iDB2ConnectionStringBuilder, " + AssemblyName;

        private readonly static Lazy<Type> connectionType = new Lazy<Type>(() => Type.GetType(ConnectionTypeName));
        
        public static Type ConnectionType => connectionType.Value;

        // https://secure.pamtransport.com/bin/IBM.Data.DB2.iSeries.xml

        public static readonly DB2iTypeDescriptor<long> BigInt = new DB2iTypeDescriptor<long>(DataType.Int64, "GetiDB2BigInt", "iDB2BigInt");
        public static readonly DB2iTypeDescriptor<byte[]> Binary = new DB2iTypeDescriptor<byte[]>(DataType.Binary, "GetiDB2Binary", "iDB2Binary");
        public static readonly DB2iTypeDescriptorConnectionCreator<byte[]> Blob = new DB2iTypeDescriptorConnectionCreator<byte[]>(DataType.Blob, "GetiDB2Blob", "iDB2Blob");
        public static readonly DB2iTypeDescriptor<string> Char = new DB2iTypeDescriptor<string>(DataType.Char, "GetiDB2Char", "iDB2Char");
        public static readonly DB2iTypeDescriptor<byte[]> CharBitData = new DB2iTypeDescriptor<byte[]>(DataType.Binary, "GetiDB2CharBitData", "iDB2CharBitData");
        public static readonly DB2iTypeDescriptorConnectionCreator<string> Clob = new DB2iTypeDescriptorConnectionCreator<string>(DataType.NText, "GetiDB2Clob", "iDB2Clob");
        public static readonly DB2iTypeDescriptor<string> DataLink = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2DataLink", "iDB2DataLink");
        public static readonly DB2iTypeDescriptor<DateTime> Date = new DB2iTypeDescriptor<DateTime>(DataType.Date, "GetiDB2Date", "iDB2Date");
        public static readonly DB2iTypeDescriptorConnectionCreator<string> DbClob = new DB2iTypeDescriptorConnectionCreator<string>(DataType.NText, "GetiDB2DbClob", "iDB2DbClob");
        public static readonly DB2iTypeDescriptor<decimal, double, long> DecFloat16 = new DB2iTypeDescriptor<decimal, double, long>(DataType.Decimal, "GetiDB2DecFloat16", "iDB2DecFloat16");
        public static readonly DB2iTypeDescriptor<decimal, double, long> DecFloat34 = new DB2iTypeDescriptor<decimal, double, long>(DataType.Decimal, "GetiDB2DecFloat34", "iDB2DecFloat34");
        public static readonly DB2iTypeDescriptor<decimal> Decimal = new DB2iTypeDescriptor<decimal>(DataType.Decimal, "GetiDB2Decimal", "iDB2Decimal");
        public static readonly DB2iTypeDescriptor<double> Double = new DB2iTypeDescriptor<double>(DataType.Double, "GetiDB2Double", "iDB2Double");
        public static readonly DB2iTypeDescriptor<string> Graphic = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2Graphic", "iDB2Graphic");
        public static readonly DB2iTypeDescriptor<int> Integer = new DB2iTypeDescriptor<int>(DataType.Int32, "GetiDB2Integer", "iDB2Integer");
        public static readonly DB2iTypeDescriptor<decimal> Numeric = new DB2iTypeDescriptor<decimal>(DataType.Decimal, "GetiDB2Numeric", "iDB2Numeric");
        public static readonly DB2iTypeDescriptor<float> Real = new DB2iTypeDescriptor<float>(DataType.Single, "GetiDB2Real", "iDB2Real");
        public static readonly DB2iTypeDescriptorDefault RowId = new DB2iTypeDescriptorDefault(typeof(byte[]), DataType.VarBinary, "GetiDB2RowId", "iDB2Rowid");
        public static readonly DB2iTypeDescriptor<short> SmallInt = new DB2iTypeDescriptor<short>(DataType.Int16, "GetiDB2SmallInt", "iDB2SmallInt");
        public static readonly DB2iTypeDescriptor<DateTime> Time = new DB2iTypeDescriptor<DateTime>(DataType.Time, "GetiDB2Time", "iDB2Time");
        public static readonly DB2iTypeDescriptor<DateTime> TimeStamp = new DB2iTypeDescriptor<DateTime>(DataType.DateTime2, "GetiDB2TimeStamp", "iDB2TimeStamp");
        public static readonly DB2iTypeDescriptor<byte[]> VarBinary = new DB2iTypeDescriptor<byte[]>(DataType.VarBinary, "GetiDB2VarBinary", "iDB2VarBinary");
        public static readonly DB2iTypeDescriptor<string> VarChar = new DB2iTypeDescriptor<string>(DataType.NVarChar, "GetiDB2VarChar", "iDB2VarChar");
        public static readonly DB2iTypeDescriptor<byte[]> VarCharBitData = new DB2iTypeDescriptor<byte[]>(DataType.VarBinary, "GetiDB2VarCharBitData", "iDB2VarCharBitData");
        public static readonly DB2iTypeDescriptor<string> VarGraphic = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2VarGraphic", "iDB2VarGraphic");
        public static readonly DB2iTypeDescriptor<string> Xml = new DB2iTypeDescriptor<string>(DataType.Xml, "GetiDB2Xml", "iDB2Xml");

        public static DB2TypeDescriptor[] AllTypes { get; }
                =
                typeof(DB2iSeriesTypes)
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


    #region

    public class DB2iTypeDescriptor : DB2iSeriesTypeDescriptorBase
    {
        public DB2iTypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2iTypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
        }

        protected override Type GetDB2Type()
        {
            return Type.GetType($"{DB2iSeriesTypes.TypesNamespaceName}.{DatatypeName},{DB2iSeriesTypes.AssemblyName}");
        }
    }

    public class DB2iTypeDescriptorDefault : DB2iTypeDescriptor
    {
        private readonly TypeCreator creator = new TypeCreator();

        public DB2iTypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2iTypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
        {
        }

        public object CreateInstance()
        {
            creator.Type = creator.Type ?? Type;
            return creator.CreateInstance();
        }
    }

    public class DB2iTypeDescriptor<T> : DB2iTypeDescriptor
    {
        private readonly TypeCreator<T> creator = new TypeCreator<T>();

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
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

    public class DB2iTypeDescriptor<T1, T2, T3> : DB2iTypeDescriptor
    {
        private readonly TypeCreator<T1, T2, T3> creator = new TypeCreator<T1, T2, T3>();

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(typeof(T1), dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
        }

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(typeof(T1), dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
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

    public class DB2iTypeDescriptorConnectionCreator<T> : DB2iTypeDescriptor
    {
        private readonly DB2iSeriesTypeCreator<T> creator = new DB2iSeriesTypeCreator<T>();

        public DB2iTypeDescriptorConnectionCreator(DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
            //creator = new Lazy<DB2iSeriesTypeCreator<T>>(() => new DB2iSeriesTypeCreator<T>(Type));
            //creator = new Lazy<DB2iSeriesTypeCreator<T>>(() => new DB2iSeriesTypeCreator<T>(
            //    Type.GetType($"{DB2iSeriesTypes.TypesNamespaceName}.{DatatypeName},{DB2iSeriesTypes.AssemblyName}")
            //    ));
            
        }

        public DB2iTypeDescriptorConnectionCreator(DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true) : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, nullValue, canBeNull)
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

    #endregion
}