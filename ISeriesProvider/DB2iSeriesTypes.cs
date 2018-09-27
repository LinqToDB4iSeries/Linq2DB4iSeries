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

        public static readonly DB2iTypeDescriptor<long> BigInt = new DB2iTypeDescriptor<long>(DataType.Int64, "GetiDB2BigInt", "iDB2BigInt", (int)iDB2DbType.iDB2BigInt);
        public static readonly DB2iTypeDescriptor<byte[]> Binary = new DB2iTypeDescriptor<byte[]>(DataType.Binary, "GetiDB2Binary", "iDB2Binary", (int)iDB2DbType.iDB2Binary);
        public static readonly DB2iTypeDescriptor<byte[]> Blob = new DB2iTypeDescriptor<byte[]>(DataType.Blob, "GetiDB2Blob", "iDB2Blob", (int)iDB2DbType.iDB2Blob);
        public static readonly DB2iTypeDescriptor<string> Char = new DB2iTypeDescriptor<string>(DataType.Char, "GetiDB2Char", "iDB2Char", (int)iDB2DbType.iDB2Char);
        public static readonly DB2iTypeDescriptor<byte[]> CharBitData = new DB2iTypeDescriptor<byte[]>(DataType.Binary, "GetiDB2CharBitData", "iDB2CharBitData", (int)iDB2DbType.iDB2CharBitData);
        public static readonly DB2iTypeDescriptor<string> Clob = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2Clob", "iDB2Clob", (int)iDB2DbType.iDB2Clob);
        public static readonly DB2iTypeDescriptor<string> DataLink = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2DataLink", "iDB2DataLink", (int)iDB2DbType.iDB2DataLink);
        public static readonly DB2iTypeDescriptor<DateTime> Date = new DB2iTypeDescriptor<DateTime>(DataType.Date, "GetiDB2Date", "iDB2Date", (int)iDB2DbType.iDB2Date);
        public static readonly DB2iTypeDescriptor<string> DbClob = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2DbClob", "iDB2DbClob", (int)iDB2DbType.iDB2Clob);
        public static readonly DB2iTypeDescriptor<decimal, double, long> DecFloat16 = new DB2iTypeDescriptor<decimal, double, long>(DataType.Decimal, "GetiDB2DecFloat16", "iDB2DecFloat16", (int)iDB2DbType.iDB2DecFloat16);
        public static readonly DB2iTypeDescriptor<decimal, double, long> DecFloat34 = new DB2iTypeDescriptor<decimal, double, long>(DataType.Decimal, "GetiDB2DecFloat34", "iDB2DecFloat34", (int)iDB2DbType.iDB2DecFloat34);
        public static readonly DB2iTypeDescriptor<decimal> Decimal = new DB2iTypeDescriptor<decimal>(DataType.Decimal, "GetiDB2Decimal", "iDB2Decimal", (int)iDB2DbType.iDB2Decimal);
        public static readonly DB2iTypeDescriptor<double> Double = new DB2iTypeDescriptor<double>(DataType.Double, "GetiDB2Double", "iDB2Double", (int)iDB2DbType.iDB2Double);
        public static readonly DB2iTypeDescriptor<string> Graphic = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2Graphic", "iDB2Graphic", (int)iDB2DbType.iDB2Graphic);
        public static readonly DB2iTypeDescriptor<int> Integer = new DB2iTypeDescriptor<int>(DataType.Int32, "GetiDB2Integer", "iDB2Integer", (int)iDB2DbType.iDB2Integer);
        public static readonly DB2iTypeDescriptor<decimal> Numeric = new DB2iTypeDescriptor<decimal>(DataType.Decimal, "GetiDB2Numeric", "iDB2Numeric", (int)iDB2DbType.iDB2Numeric);
        public static readonly DB2iTypeDescriptor<float> Real = new DB2iTypeDescriptor<float>(DataType.Single, "GetiDB2Real", "iDB2Real", (int)iDB2DbType.iDB2Real);
        public static readonly DB2iTypeDescriptorDefault RowId = new DB2iTypeDescriptorDefault(typeof(byte[]), DataType.VarBinary, "GetiDB2RowId", "iDB2Rowid", (int)iDB2DbType.iDB2Rowid);
        public static readonly DB2iTypeDescriptor<short> SmallInt = new DB2iTypeDescriptor<short>(DataType.Int16, "GetiDB2SmallInt", "iDB2SmallInt", (int)iDB2DbType.iDB2SmallInt);
        public static readonly DB2iTypeDescriptor<DateTime> Time = new DB2iTypeDescriptor<DateTime>(DataType.Time, "GetiDB2Time", "iDB2Time", (int)iDB2DbType.iDB2Time);
        public static readonly DB2iTypeDescriptor<DateTime> TimeStamp = new DB2iTypeDescriptor<DateTime>(DataType.DateTime2, "GetiDB2TimeStamp", "iDB2TimeStamp", (int)iDB2DbType.iDB2TimeStamp);
        public static readonly DB2iTypeDescriptor<byte[]> VarBinary = new DB2iTypeDescriptor<byte[]>(DataType.VarBinary, "GetiDB2VarBinary", "iDB2VarBinary", (int)iDB2DbType.iDB2VarBinary);
        public static readonly DB2iTypeDescriptor<string> VarChar = new DB2iTypeDescriptor<string>(DataType.NVarChar, "GetiDB2VarChar", "iDB2VarChar", (int)iDB2DbType.iDB2VarChar);
        public static readonly DB2iTypeDescriptor<byte[]> VarCharBitData = new DB2iTypeDescriptor<byte[]>(DataType.VarBinary, "GetiDB2VarCharBitData", "iDB2VarCharBitData", (int)iDB2DbType.iDB2VarCharBitData);
        public static readonly DB2iTypeDescriptor<string> VarGraphic = new DB2iTypeDescriptor<string>(DataType.NText, "GetiDB2VarGraphic", "iDB2VarGraphic", (int)iDB2DbType.iDB2VarGraphic);
        public static readonly DB2iTypeDescriptor<string> Xml = new DB2iTypeDescriptor<string>(DataType.Xml, "GetiDB2Xml", "iDB2Xml", (int)iDB2DbType.iDB2Xml);

        public static DB2iTypeDescriptor[] AllTypes { get; }
                =
                typeof(DB2iSeriesTypes)
                    .GetPropertiesEx()
                    .Where(x => x.PropertyType == typeof(DB2iTypeDescriptor))
                    .Select(x => (DB2iTypeDescriptor)x.GetValue(null))
                    .ToArray();

        private static readonly Lazy<Func<string, DbConnectionStringBuilder>> dbConnectionStringBuilderCreator
           = new Lazy<Func<string, DbConnectionStringBuilder>>(() =>
               TypeCreatorBuilder.BuildStaticTypeCreator<string, DbConnectionStringBuilder>(
                 actualType: Type.GetType(TypeNameDbConnectionStringBuilder, true)));

        public static DbConnectionStringBuilder CreateConnectionStringBuilder(string connectionString = null)
            => dbConnectionStringBuilderCreator.Value(connectionString ?? "");

        #region iDB2Type for iDB2Parameter

        //Taken from IBM.Data.DB2.iSeries
        private enum iDB2DbType
        {
            iDB2BigInt = 1,
            iDB2Integer = 2,
            iDB2SmallInt = 3,
            iDB2Decimal = 4,
            iDB2Numeric = 5,
            iDB2Char = 6,
            iDB2VarChar = 7,
            iDB2CharBitData = 8,
            iDB2VarCharBitData = 9,
            iDB2Graphic = 10,
            iDB2VarGraphic = 11,
            iDB2Date = 12,
            iDB2Time = 13,
            iDB2TimeStamp = 14,
            iDB2Rowid = 15,
            iDB2Real = 16,
            iDB2Double = 17,
            iDB2Binary = 18,
            iDB2VarBinary = 19,
            iDB2Blob = 20,
            iDB2Clob = 21,
            iDB2DbClob = 22,
            iDB2DataLink = 23,
            iDB2DecFloat16 = 24,
            iDB2DecFloat34 = 25,
            iDB2Xml = 26
        }

        #endregion
    }


    #region DB2iTypeDescriptor

    public class DB2iTypeDescriptor : DB2iSeriesTypeDescriptorBase
    {
        public DB2iTypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull)
        {
        }

        public DB2iTypeDescriptor(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull)
        {
        }

        protected override Type GetDB2Type()
        {
            return Type.GetType($"{DB2iSeriesTypes.TypesNamespaceName}.{DatatypeName},{DB2iSeriesTypes.AssemblyName}");
        }
    }

    public class DB2iTypeDescriptorDefault : DB2iTypeDescriptor
    {
        private readonly Lazy<Func<object>> creator;

        public DB2iTypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull)
        {
            creator = new Lazy<Func<object>>(() => TypeCreatorBuilder.BuildTypeCreator(Type));
        }

        public DB2iTypeDescriptorDefault(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true)
            : base(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull)
        {
            creator = new Lazy<Func<object>>(() => TypeCreatorBuilder.BuildTypeCreator(Type));
        }

        public object CreateInstance()
        {
            return creator.Value();
        }
    }

    public class DB2iTypeDescriptor<T> : DB2iTypeDescriptorDefault
    {
        private readonly Lazy<Func<T, object>> creator;

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true)
            : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull)
        {
            creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
        }

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true)
            : base(typeof(T), dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull)
        {
            creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
        }

        public object CreateInstance(T value)
        {
            return creator.Value(value);
        }
    }

    public class DB2iTypeDescriptor<T1, T> : DB2iTypeDescriptor<T1>
    {
        private readonly Lazy<Func<T, object>> creator;

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull)
        {
            creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
        }

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull)
        {
            creator = new Lazy<Func<T, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T>(Type));
        }

        public object CreateInstance(T value)
        {
            return creator.Value(value);
        }
    }

    public class DB2iTypeDescriptor<T1, T2, T3> : DB2iTypeDescriptor<T1, T2>
    {
        private readonly Lazy<Func<T3, object>> creator;

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull)
        {
            creator = new Lazy<Func<T3, object>>(() => TypeCreatorBuilder.BuildTypeCreator<T3>(Type));
        }

        public DB2iTypeDescriptor(DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true)
            : base(dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, nullValue, canBeNull)
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