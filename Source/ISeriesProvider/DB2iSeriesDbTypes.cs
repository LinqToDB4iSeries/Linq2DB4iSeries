using LinqToDB.Common;
using LinqToDB.Mapping;
using LinqToDB.SqlQuery;
using System;
using System.Collections.Generic;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class DB2iSeriesDbTypes
	{
		internal class DbTypeInfo
		{
			public DbTypeInfo(
				string name,
				DataType dataType,
				int? defaultLength = null,
				int? defaultPrecision = null,
				int? defaultScale = null,
				int? maxLength = null,
				int? maxPrecision = null,
				int? maxScale = null,
				bool requiresLength = false,
				bool requiresPrecision = false,
				bool requiresScale = false,
				bool forBitData = false)
			{
				Name = name;
				DataType = dataType;
				DefaultLength = defaultLength;
				DefaultPrecision = defaultPrecision;
				DefaultScale = defaultScale;
				MaxLength = maxLength;
				MaxPrecision = maxPrecision;
				MaxScale = maxScale;
				RequiresLength = requiresLength;
				RequiresPrecision = requiresPrecision;
				RequiresScale = requiresScale;
				ForBitData = forBitData;
			}

			public string Name { get; }
			public DataType DataType { get; }
			public int? DefaultLength { get; }
			public int? DefaultPrecision { get; }
			public int? DefaultScale { get; }
			public int? MaxLength { get; }
			public int? MaxPrecision { get; }
			public int? MaxScale { get; }
			public bool RequiresLength { get; }
			public bool RequiresPrecision { get; }
			public bool RequiresScale { get; }
			public bool ForBitData { get; }

			public bool HasLength => DefaultLength.HasValue;
			public bool HasPrecision => DefaultPrecision.HasValue;
			public bool HasScale => DefaultPrecision.HasValue && DefaultScale.HasValue;
		}

		public readonly static DbTypeInfo DbBinary = new DbTypeInfo(Constants.DbTypes.Binary, DataType.Binary, defaultLength: 4000, maxLength: 32740, requiresLength: true);
		public readonly static DbTypeInfo DbVarBinary = new DbTypeInfo(Constants.DbTypes.VarBinary, DataType.VarBinary, defaultLength: 4000, maxLength: 32740, requiresLength: true);
		public readonly static DbTypeInfo DbBlob = new DbTypeInfo(Constants.DbTypes.Blob, DataType.Blob, defaultLength: 1048576, maxLength: 2147483647, requiresLength: true);

		public readonly static DbTypeInfo DbChar = new DbTypeInfo(Constants.DbTypes.Char, DataType.Char, defaultLength: 255, maxLength: 32740, requiresLength: true);
		public readonly static DbTypeInfo DbVarChar = new DbTypeInfo(Constants.DbTypes.VarChar, DataType.VarChar, defaultLength: 255, maxLength: 32740, requiresLength: true);
		public readonly static DbTypeInfo DbClob = new DbTypeInfo(Constants.DbTypes.Clob, DataType.Text, defaultLength: 1048576, maxLength: 2147483647, requiresLength: true);

		public readonly static DbTypeInfo DbNChar = new DbTypeInfo(Constants.DbTypes.NChar, DataType.NChar, defaultLength: 255, maxLength: 16370, requiresLength: true);
		public readonly static DbTypeInfo DbNVarChar = new DbTypeInfo(Constants.DbTypes.NVarChar, DataType.NVarChar, defaultLength: 255, maxLength: 16370, requiresLength: true);
		public readonly static DbTypeInfo DbNClob = new DbTypeInfo(Constants.DbTypes.NClob, DataType.NText, defaultLength: 1048576, maxLength: 1073741823, requiresLength: true);

		public readonly static DbTypeInfo DbGraphic = new DbTypeInfo(Constants.DbTypes.Graphic, DataType.NChar, defaultLength: 255, maxLength: 16370, requiresLength: true);
		public readonly static DbTypeInfo DbVarGraphic = new DbTypeInfo(Constants.DbTypes.VarGraphic, DataType.NVarChar, defaultLength: 255, maxLength: 16370, requiresLength: true);
		public readonly static DbTypeInfo DbDbClob = new DbTypeInfo(Constants.DbTypes.DbClob, DataType.NText, defaultLength: 1048576, maxLength: 1073741823, requiresLength: true);

		public readonly static DbTypeInfo DbSmallInt = new DbTypeInfo(Constants.DbTypes.SmallInt, DataType.Int16);
		public readonly static DbTypeInfo DbInteger = new DbTypeInfo(Constants.DbTypes.Integer, DataType.Int32);
		public readonly static DbTypeInfo DbBigInt = new DbTypeInfo(Constants.DbTypes.BigInt, DataType.Int64);

		public readonly static DbTypeInfo DbDecimal = new DbTypeInfo(Constants.DbTypes.Decimal, DataType.Decimal, defaultPrecision: 29, defaultScale: 10, maxPrecision: 63, maxScale: 60, requiresPrecision: true, requiresScale: true);
		public readonly static DbTypeInfo DbReal = new DbTypeInfo(Constants.DbTypes.Real, DataType.Single, defaultLength: 4, maxLength: 4);
		public readonly static DbTypeInfo DbFloat = new DbTypeInfo(Constants.DbTypes.Float, DataType.Double, defaultLength: 8, maxLength: 8);
		public readonly static DbTypeInfo DbDouble = new DbTypeInfo(Constants.DbTypes.Double, DataType.Double, defaultLength: 8, maxLength: 8);

		public readonly static DbTypeInfo DbTimestamp = new DbTypeInfo(Constants.DbTypes.TimeStamp, DataType.DateTime, defaultPrecision: 6, maxPrecision: 12);
		public readonly static DbTypeInfo DbDate = new DbTypeInfo(Constants.DbTypes.Date, DataType.Date);
		public readonly static DbTypeInfo DbTime = new DbTypeInfo(Constants.DbTypes.Time, DataType.Time);

		public readonly static DbTypeInfo DbChar16ForBitData = new DbTypeInfo(Constants.DbTypes.Char16ForBitData, DataType.Guid, forBitData: true);

		public readonly static DbTypeInfo DbDataLink = new DbTypeInfo(Constants.DbTypes.DataLink, DataType.Undefined);
		public readonly static DbTypeInfo DbRowId = new DbTypeInfo(Constants.DbTypes.RowId, DataType.Undefined);

		public static readonly IReadOnlyDictionary<string, DbTypeInfo> DbTypes = new DbTypeInfo[]
		{
			DbBinary, DbVarBinary, DbBlob,
			DbChar, DbVarChar, DbClob,
			DbNChar, DbNVarChar, DbNClob,
			DbGraphic, DbVarGraphic, DbDbClob,

			DbSmallInt, DbInteger, DbBigInt,

			DbDecimal, DbReal,DbFloat, DbDouble,

			DbTimestamp, DbDate, DbTime,

			DbDataLink, DbRowId,

			DbChar16ForBitData
		}
		.ToDictionary(x => x.Name);

		public static readonly IReadOnlyDictionary<DataType, DbTypeInfo> DataTypeMap = new Dictionary<DataType, DbTypeInfo>
		{
			{ DataType.Binary, DbTypes[Constants.DbTypes.Binary] },
			{ DataType.VarBinary, DbTypes[Constants.DbTypes.VarBinary] },
			{ DataType.Blob, DbTypes[Constants.DbTypes.Blob] },

			{ DataType.Char, DbTypes[Constants.DbTypes.Char] },
			{ DataType.VarChar, DbTypes[Constants.DbTypes.VarChar] },
			{ DataType.Text, DbTypes[Constants.DbTypes.Clob] },

			{ DataType.NChar, DbTypes[Constants.DbTypes.NChar] },
			{ DataType.NVarChar, DbTypes[Constants.DbTypes.NVarChar] },
			{ DataType.NText, DbTypes[Constants.DbTypes.NClob] },

			{ DataType.Boolean, DbTypes[Constants.DbTypes.SmallInt] },
			{ DataType.Byte, DbTypes[Constants.DbTypes.SmallInt] },
			{ DataType.SByte, DbTypes[Constants.DbTypes.SmallInt] },
			{ DataType.Int16, DbTypes[Constants.DbTypes.SmallInt] },

			{ DataType.UInt16, DbTypes[Constants.DbTypes.Integer] },
			{ DataType.Int32, DbTypes[Constants.DbTypes.Integer] },

			{ DataType.UInt32, DbTypes[Constants.DbTypes.BigInt] },
			{ DataType.Int64, DbTypes[Constants.DbTypes.BigInt] },

			{ DataType.UInt64, DbTypes[Constants.DbTypes.Decimal] },
			{ DataType.Decimal, DbTypes[Constants.DbTypes.Decimal] },

			{ DataType.Single, DbTypes[Constants.DbTypes.Real] },
			{ DataType.Double, DbTypes[Constants.DbTypes.Double] },

			{ DataType.DateTimeOffset, DbTypes[Constants.DbTypes.TimeStamp] },
			{ DataType.Timestamp, DbTypes[Constants.DbTypes.TimeStamp] },
			{ DataType.DateTime, DbTypes[Constants.DbTypes.TimeStamp] },
			{ DataType.DateTime2, DbTypes[Constants.DbTypes.TimeStamp] },

			{ DataType.Date, DbTypes[Constants.DbTypes.Date] },
			{ DataType.Time, DbTypes[Constants.DbTypes.Time] },

			{ DataType.Guid, DbTypes[Constants.DbTypes.Char16ForBitData] },
		};

		public static DbDataType GetDbDataType(DbTypeInfo dbTypeInfo, Type systemType, int? length, int? precision, int? scale, bool forceDefaultAttributes, bool supportsNCharTypes)
		{
			static int? sanitize(int? parameter, int minValue, bool supported)
			{
				if (!supported || parameter < minValue) return null;
				return parameter;
			}

			if (forceDefaultAttributes)
			{
				length = dbTypeInfo.RequiresLength ? dbTypeInfo.DefaultLength : null;
				precision = dbTypeInfo.RequiresPrecision ? dbTypeInfo.DefaultPrecision : null;
				scale = dbTypeInfo.RequiresPrecision ? dbTypeInfo.DefaultScale : null;
			}
			else
			{
				length = sanitize(length, 1, dbTypeInfo.HasLength);
				precision = sanitize(precision, 0, dbTypeInfo.HasPrecision);
				scale = sanitize(scale, 0, dbTypeInfo.HasScale);

				length ??= dbTypeInfo.RequiresLength ? dbTypeInfo.DefaultLength : null;
				precision ??= dbTypeInfo.RequiresPrecision ? dbTypeInfo.DefaultPrecision : null;
				scale = precision.HasValue ? scale ?? (dbTypeInfo.RequiresPrecision ? dbTypeInfo.DefaultScale : null) : null;
			}


			if (dbTypeInfo.ForBitData)
			{
				var name = dbTypeInfo.Name.Contains("(") ?
					dbTypeInfo.Name : $"{dbTypeInfo.Name}({length}) FOR BIT DATA";

				return new DbDataType(systemType, dbTypeInfo.DataType, name);
			}
			else
			{
				var name = dbTypeInfo.Name;

				if (!supportsNCharTypes)
				{
					if (dbTypeInfo.Name.StartsWith(Constants.DbTypes.NChar))
					{
						name = Constants.DbTypes.GraphicUnicode;
					}
					else if (dbTypeInfo.Name.StartsWith(Constants.DbTypes.NVarChar))
					{
						name = Constants.DbTypes.VarGraphicUnicode;
					}
					else if (dbTypeInfo.Name.StartsWith(Constants.DbTypes.NClob))
					{
						name = Constants.DbTypes.DBClobUnicode;
					}
				}
				return new DbDataType(
					systemType,
					dbTypeInfo.DataType,
					name,
					length, precision, scale);
			}
		}

		public static DbDataType GetDbDataType(Type systemType, DataType dataType, int? length, int? precision, int? scale, bool mapGuidAsString, bool forceDefaultAttributes, bool supportsNCharTypes)
		{
			if (!DataTypeMap.TryGetValue(dataType, out var dbTypeInfo))
			{
				return new DbDataType(systemType, DataType.Undefined);
			}

			return dataType switch
			{
				//Decimal(29)
				DataType.UInt64 =>
					GetDbDataType(dbTypeInfo, systemType, null, 29, 0, forceDefaultAttributes, supportsNCharTypes),

				//When defaults request get a the default Decimal type, 
				//else set defaults to Decimal(60,30) to fit any value
				DataType.Decimal =>
					forceDefaultAttributes ?
						GetDbDataType(dbTypeInfo, systemType, null, null, null, true, supportsNCharTypes) :
						GetDbDataType(dbTypeInfo, systemType, null, 60, 30, false, supportsNCharTypes),

				//Depending on mapping
				DataType.Guid =>
					mapGuidAsString ?
						new DbDataType(systemType, dataType, Constants.DbTypes.VarChar, 38) :
						GetDbDataType(dbTypeInfo, systemType, null, null, null, true, supportsNCharTypes),

				//Any other type, fall back to DbTypeInfo configuration
				_ =>
					GetDbDataType(dbTypeInfo, systemType, length, precision, scale, forceDefaultAttributes, supportsNCharTypes),
			};
		}

		public static DbDataType GetDbTypeForCast(SqlDataType type, MappingSchema mappingSchema, DB2iSeriesSqlProviderFlags flags)
		{
			if (!string.IsNullOrEmpty(type.Type.DbType))
			{
				var parenthesisIndex = type.Type.DbType.IndexOf('(');
				var dbType = (parenthesisIndex >= 0 ? type.Type.DbType.Substring(0, parenthesisIndex) : type.Type.DbType)
						.ToUpper();

				var forBitData = type.Type.DbType.Substring(0, type.Type.DbType.IndexOf(')') + 1).Length > 0;

				//Upcast types that require length/precision to fit value
				switch (dbType)
				{
					//Upcast all char/string types to unicode for compatibility with .net string type
					//type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.Text, Constants.DbTypes.Clob));
					//break;
					case Constants.DbTypes.Char when !forBitData:
					case Constants.DbTypes.VarChar when !forBitData:
					case Constants.DbTypes.Clob:
					case Constants.DbTypes.NChar:
					case Constants.DbTypes.NVarChar:
					case Constants.DbTypes.NClob:
					case Constants.DbTypes.Graphic:
					case Constants.DbTypes.VarGraphic:
					case Constants.DbTypes.Varg:
					case Constants.DbTypes.DbClob:
						type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.NText, Constants.DbTypes.DBClobUnicode));
						break;
					case Constants.DbTypes.Binary:
					case Constants.DbTypes.VarBinary:
					case Constants.DbTypes.Blob:
						type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.Blob, Constants.DbTypes.Blob));
						break;
					default:
						break;
				}
			}
			else
			{
				if (type.Type.DataType == DataType.Undefined)
					type = mappingSchema.GetTypeOrUnderlyingTypeDataType(type.Type.SystemType);

				type = new SqlDataType(
					GetDbDataType(type.SystemType, type.Type.DataType, type.Type.Length, type.Type.Precision, type.Type.Scale, mappingSchema.IsGuidMappedAsString(), false, true));

				//Upcast types that require length/precision to fit value
				switch (type.Type.DataType)
				{
					case DataType.Undefined:
						break;
					case DataType.Char:
					case DataType.VarChar:
					case DataType.Text:
						type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.Text, Constants.DbTypes.Clob));
						break;
					case DataType.NChar:
					case DataType.NVarChar:
					case DataType.NText:
						type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.NText, Constants.DbTypes.DBClobUnicode));
						break;
					case DataType.Binary:
					case DataType.VarBinary:
					case DataType.Blob:
						type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.Text, Constants.DbTypes.Blob));
						break;
					case DataType.Decimal:
						var p = type.Type.Precision ?? type.Type.Length;
						var s = p.HasValue ? type.Type.Scale : 0;
						type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.Decimal, Constants.DbTypes.Decimal, null, p ?? 60, s ?? 30));
						break;
					case DataType.DateTime:
					case DataType.DateTimeOffset:
					case DataType.DateTime2:
						if(flags.SupportsArbitraryTimeStampPercision)
							type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.DateTime, Constants.DbTypes.TimeStamp, null, type.Type.Precision, null));
						else
							type = new SqlDataType(new DbDataType(type.Type.SystemType, DataType.DateTime, Constants.DbTypes.TimeStamp, null, null, null));
						break;
					default: break;
				}
			}

			return type.Type;
		}
	}
}
