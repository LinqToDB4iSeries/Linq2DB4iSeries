using LinqToDB;
using LinqToDB.Mapping;
using System;

#nullable disable

namespace Tests.DataProvider
{
	public partial class DB2iSeriesTests
	{
		enum TestEnum
		{
			[MapValue("A")] AA,
			[MapValue("B")] BB,
		}

		[Table(Name = "ALLTYPES")]
		public class ALLTYPE
		{
			[PrimaryKey, Identity]
			public int ID { get; set; } // INTEGER

			[Column(DbType = "bigint"), Nullable]
			public long? BIGINTDATATYPE { get; set; } // BIGINT

			[Column(DbType = "int"), Nullable]
			public int? INTDATATYPE { get; set; } // INTEGER

			[Column(DbType = "smallint"), Nullable]
			public short? SMALLINTDATATYPE { get; set; } // SMALLINT

			[Column(DbType = "decimal(30)"), Nullable]
			public decimal? DECIMALDATATYPE { get; set; } // DECIMAL

			[Column(DbType = "decfloat(16)"), Nullable]
			public decimal? DECFLOAT16DATATYPE { get; set; } // DECFLOAT16

			[Column(DbType = "decfloat(34)"), Nullable]
			public decimal? DECFLOAT34DATATYPE { get; set; } // DECFLOAT34

			[Column(DbType = "real"), Nullable]
			public float? REALDATATYPE { get; set; } // REAL

			[Column(DbType = "double"), Nullable]
			public double? DOUBLEDATATYPE { get; set; } // DOUBLE

			[Column(DbType = "char(1)"), Nullable]
			public char CHARDATATYPE { get; set; } // CHARACTER

			[Column(DbType = "varchar(20)"), Nullable]
			public string VARCHARDATATYPE { get; set; } // VARCHAR(20)

			[Column(DbType = "clob"), Nullable]
			public string CLOBDATATYPE { get; set; } // CLOB(1048576)

			[Column(DbType = "dbclob(100)"), Nullable]
			public string DBCLOBDATATYPE { get; set; } // DBCLOB(100)

			[Column(DbType = "binary(20)"), Nullable]
			public object BINARYDATATYPE { get; set; } // BINARY(20)

			[Column(DbType = "varbinary(20)"), Nullable]
			public object VARBINARYDATATYPE { get; set; } // VARBINARY(20)

			[Column(DbType = "blob"), Nullable]
			public byte[] BLOBDATATYPE { get; set; } // BLOB(10)

			[Column(DbType = "graphic(10)"), Nullable]
			public string GRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "vargraphic(10)"), Nullable]
			public string VARGRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "date"), Nullable]
			public DateTime? DATEDATATYPE { get; set; } // DATE

			[Column(DbType = "time"), Nullable]
			public TimeSpan? TIMEDATATYPE { get; set; } // TIME

			[Column(DbType = "timestamp"), Nullable]
			public DateTime? TIMESTAMPDATATYPE { get; set; } // TIMESTAMP

			[Column, Nullable]
			public string XMLDATATYPE { get; set; } // XML
		}

		[Table(Name = "ALLTYPES2")]
		public class ALLTYPE2
		{
			[PrimaryKey, Identity]
			public int ID { get; set; } // INTEGER

			[Column(DbType = "bigint"), Nullable]
			public long? BIGINTDATATYPE { get; set; } // BIGINT

			[Column(DbType = "int"), Nullable]
			public int? INTDATATYPE { get; set; } // INTEGER

			[Column(DbType = "smallint"), Nullable]
			public short? SMALLINTDATATYPE { get; set; } // SMALLINT

			[Column(DbType = "decimal(30)"), Nullable]
			public decimal? DECIMALDATATYPE { get; set; } // DECIMAL

			[Column(DbType = "decfloat(16)"), Nullable]
			public decimal? DECFLOAT16DATATYPE { get; set; } // DECFLOAT16

			[Column(DbType = "decfloat(34)"), Nullable]
			public decimal? DECFLOAT34DATATYPE { get; set; } // DECFLOAT34

			[Column(DbType = "real"), Nullable]
			public float? REALDATATYPE { get; set; } // REAL

			[Column(DbType = "double"), Nullable]
			public double? DOUBLEDATATYPE { get; set; } // DOUBLE

			[Column(DbType = "char(1)"), Nullable]
			public char CHARDATATYPE { get; set; } // CHARACTER

			[Column(DbType = "varchar(20)"), Nullable]
			public string VARCHARDATATYPE { get; set; } // VARCHAR(20)

			[Column(DbType = "graphic(10)"), Nullable]
			public string GRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "vargraphic(10)"), Nullable]
			public string VARGRAPHICDATATYPE { get; set; } // GRAPHIC(10)

			[Column(DbType = "binary(20)"), Nullable]
			public object BINARYDATATYPE { get; set; } // BINARY(20)

			[Column(DbType = "varbinary(20)"), Nullable]
			public object VARBINARYDATATYPE { get; set; } // VARBINARY(20)

			[Column(DbType = "date"), Nullable]
			public DateTime? DATEDATATYPE { get; set; } // DATE

			[Column(DbType = "time"), Nullable]
			public TimeSpan? TIMEDATATYPE { get; set; } // TIME

			[Column(DbType = "timestamp"), Nullable]
			public DateTime? TIMESTAMPDATATYPE { get; set; } // TIMESTAMP
		}

		[Table("InsertOrUpdateByte")]
		class MergeTypesByte
		{
			[Column("Id", IsIdentity = true)] [PrimaryKey] public int Id { get; set; }

			[Column("FieldByteAsDecimal", DataType = DataType.Decimal, Length = 2, Precision = 0)] public byte FieldByte { get; set; }

			[Column("FieldULongAsDecimal", DataType = DataType.Decimal, Length = 20, Precision = 0)] public ulong FieldULong { get; set; }
		}
	}
}
