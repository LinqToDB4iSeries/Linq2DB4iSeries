using System;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using Data;
    using Configuration;
    using Extensions;
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

    public class DB2Types
    {
        //internal const string AssemblyName_DB2Connect_Net = "IBM.Data.DB2";
        //internal const string AssemblyName_DB2Connect_Core = "IBM.Data.DB2.Core";
        internal const string NamespaceNameDB2Types = "IBM.Data.DB2Types";

        internal static readonly string AssemblyName = DB2.DB2Tools.AssemblyName;
        internal static readonly string NamespaceName = AssemblyName;

        internal static readonly string ConnectionTypeName = NamespaceName + ".DB2Connection";
        internal static readonly string DataReaderTypeName = NamespaceName + ".DB2DataReader, " + AssemblyName;

        internal static readonly string TypeNameDbConnectionStringBuilder = AssemblyName + ".DB2ConnectionStringBuilder, " + AssemblyName;

        private readonly static Lazy<DB2Types> instance = new Lazy<DB2Types>(() => new DB2Types());
        public static DB2Types Instance => instance.Value;

        public DB2.TypeCreator<long> DB2Int64 { get; } = new DB2.TypeCreator<long>();
        public DB2.TypeCreator DB2RowId { get; } = new DB2.TypeCreator();
        public DB2.TypeCreator<DateTime> DB2TimeStamp { get; } = new DB2.TypeCreator<DateTime>();
        public DB2.TypeCreator<TimeSpan> DB2Time { get; } = new DB2.TypeCreator<TimeSpan>();
        public DB2.TypeCreator<DateTime, long> DB2DateTime { get; } = new DB2.TypeCreator<DateTime, long>();
        public DB2.TypeCreator<DateTime> DB2Date { get; } = new DB2.TypeCreator<DateTime>();
        public DB2.ConnectionTypeTypeCreator<byte[]> DB2Blob { get; } = new DB2.ConnectionTypeTypeCreator<byte[]>();
        public DB2.TypeCreator<byte[]> DB2Binary { get; } = new DB2.TypeCreator<byte[]>();
        public DB2.ConnectionTypeTypeCreator<string> DB2Clob { get; } = new DB2.ConnectionTypeTypeCreator<string>();
        public DB2.TypeCreator<double> DB2Double { get; } = new DB2.TypeCreator<double>();
        public DB2.TypeCreator<double> DB2Real370 { get; } = new DB2.TypeCreator<double>();
        public DB2.TypeCreator<float> DB2Real { get; } = new DB2.TypeCreator<float>();
        public DB2.TypeCreator<decimal, double, long> DB2DecimalFloat { get; } = new DB2.TypeCreator<decimal, double, long>();
        public DB2.TypeCreator<decimal> DB2Decimal { get; } = new DB2.TypeCreator<decimal>();
        public DB2.TypeCreator<short> DB2Int16 { get; } = new DB2.TypeCreator<short>();
        public DB2.TypeCreator<int> DB2Int32 { get; } = new DB2.TypeCreator<int>();
        public DB2.TypeCreator<string> DB2String { get; } = new DB2.TypeCreator<string>();
        public Type DB2Xml { get; } 
        public Type ConnectionType { get; }

        public DB2Types()
            :this(AutoLoadAssembly())
        {

        }

        public DB2Types(Assembly assembly)
        {
            Type getType(string typeName) => assembly.GetType($"{NamespaceNameDB2Types}.{typeName}", true);

            DB2Int64.Type = getType("DB2Int64");
            DB2Int32.Type = getType("DB2Int32");
            DB2Int16.Type = getType("DB2Int16");
            DB2Decimal.Type = getType("DB2Decimal");
            DB2DecimalFloat.Type = getType("DB2DecimalFloat");
            DB2Real.Type = getType("DB2Real");
            DB2Real370.Type = getType("DB2Real370");
            DB2Double.Type = getType("DB2Double");
            DB2String.Type = getType("DB2String");
            DB2Clob.Type = getType("DB2Clob");
            DB2Binary.Type = getType("DB2Binary");
            DB2Blob.Type = getType("DB2Blob");
            DB2Date.Type = getType("DB2Date");
            DB2Time.Type = getType("DB2Time");
            DB2TimeStamp.Type = getType("DB2TimeStamp");
            DB2Xml = getType("DB2Xml");
            DB2RowId.Type = getType("DB2RowId");

            ConnectionType = assembly.GetType(ConnectionTypeName);
        }

        private static Assembly AutoLoadAssembly()
        {
            return Assembly.LoadFile(System.IO.Path.GetDirectoryName(Assembly.GetCallingAssembly().Location) + "\\" + AssemblyName + ".dll");
        }
    }
}