using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

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
				bool requiresPrecisionScale = false,
				bool forBitData = false,
				bool isComposite = false)
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
				RequiresPrecisionScale = requiresPrecisionScale;
				ForBitData = forBitData;
				IsComposite = isComposite;
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
			public bool RequiresPrecisionScale { get; }
			public bool RequiresScale { get; }
			public bool ForBitData { get; }
			public bool	IsComposite { get; }

			public bool HasLength => DefaultLength.HasValue;
			public bool HasPrecision => DefaultPrecision.HasValue;
			public bool HasScale => DefaultPrecision.HasValue && DefaultScale.HasValue;
		}

		private const int MAX_CHAR_LENGTH = 32740;
		private const int MAX_BINARY_LENGTH = 32740;
		private const int MAX_NCHAR_LENGTH = 32740 / 2;
		
		public readonly static DbTypeInfo DbBinary = new(Constants.DbTypes.Binary, DataType.Binary, defaultLength: 4000, maxLength: MAX_BINARY_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbVarBinary = new(Constants.DbTypes.VarBinary, DataType.VarBinary, defaultLength: 4000, maxLength: MAX_BINARY_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbBlob = new(Constants.DbTypes.Blob, DataType.Blob, defaultLength: 1048576, maxLength: 2147483647, requiresLength: true);

		public readonly static DbTypeInfo DbChar = new(Constants.DbTypes.Char, DataType.Char, defaultLength: 255, maxLength: MAX_CHAR_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbVarChar = new(Constants.DbTypes.VarChar, DataType.VarChar, defaultLength: 255, maxLength: MAX_CHAR_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbClob = new(Constants.DbTypes.Clob, DataType.Text, defaultLength: 1048576, maxLength: 2147483647, requiresLength: true);

		public readonly static DbTypeInfo DbNChar = new(Constants.DbTypes.NChar, DataType.NChar, defaultLength: 255, maxLength: MAX_NCHAR_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbNVarChar = new(Constants.DbTypes.NVarChar, DataType.NVarChar, defaultLength: 255, maxLength: MAX_NCHAR_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbNClob = new(Constants.DbTypes.NClob, DataType.NText, defaultLength: 1048576, maxLength: 1073741823, requiresLength: true);

		public readonly static DbTypeInfo DbGraphic = new(Constants.DbTypes.Graphic, DataType.NChar, defaultLength: 255, maxLength: MAX_NCHAR_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbVarGraphic = new(Constants.DbTypes.VarGraphic, DataType.NVarChar, defaultLength: 255, maxLength: MAX_NCHAR_LENGTH, requiresLength: true);
		public readonly static DbTypeInfo DbDbClob = new(Constants.DbTypes.DbClob, DataType.NText, defaultLength: 1048576, maxLength: 1073741823, requiresLength: true);

		public readonly static DbTypeInfo DbSmallInt = new(Constants.DbTypes.SmallInt, DataType.Int16);
		public readonly static DbTypeInfo DbInteger = new(Constants.DbTypes.Integer, DataType.Int32);
		public readonly static DbTypeInfo DbBigInt = new(Constants.DbTypes.BigInt, DataType.Int64);

		public readonly static DbTypeInfo DbDecimal = new(Constants.DbTypes.Decimal, DataType.Decimal, defaultPrecision: 29, defaultScale: 10, maxPrecision: 63, maxScale: 60, requiresPrecisionScale: true);
		public readonly static DbTypeInfo DbReal = new(Constants.DbTypes.Real, DataType.Single, defaultLength: 4, maxLength: 4);
		public readonly static DbTypeInfo DbFloat = new(Constants.DbTypes.Float, DataType.Double, defaultLength: 8, maxLength: 8);
		public readonly static DbTypeInfo DbDouble = new(Constants.DbTypes.Double, DataType.Double, defaultLength: 8, maxLength: 8);

		public readonly static DbTypeInfo DbTimestamp = new(Constants.DbTypes.TimeStamp, DataType.DateTime, defaultPrecision: 6, maxPrecision: 12);
		public readonly static DbTypeInfo DbDate = new(Constants.DbTypes.Date, DataType.Date);
		public readonly static DbTypeInfo DbTime = new(Constants.DbTypes.Time, DataType.Time);

		public readonly static DbTypeInfo DbChar16ForBitData = new(Constants.DbTypes.Char16ForBitData, DataType.Guid, defaultLength: 16, maxLength: 16, forBitData: true, isComposite: true);

		public readonly static DbTypeInfo DbDataLink = new(Constants.DbTypes.DataLink, DataType.Undefined);
		public readonly static DbTypeInfo DbRowId = new(Constants.DbTypes.RowId, DataType.Undefined);

		public static readonly IReadOnlyDictionary<string, DbTypeInfo> DbTypeInfos = new DbTypeInfo[]
		{
			DbBinary, DbVarBinary, DbBlob,
			DbChar, DbVarChar, DbClob,
			DbNChar, DbNVarChar, DbNClob,
			DbGraphic, DbVarGraphic, DbDbClob,

			DbSmallInt, DbInteger, DbBigInt,

			DbDecimal, DbReal, DbFloat, DbDouble,

			DbTimestamp, DbDate, DbTime,

			DbDataLink, DbRowId,

			DbChar16ForBitData
		}
		.ToDictionary(x => x.Name);

		public static readonly IReadOnlyDictionary<DataType, DbTypeInfo> DataTypeMap = new Dictionary<DataType, DbTypeInfo>
		{
			{ DataType.Binary, DbTypeInfos[Constants.DbTypes.Binary] },
			{ DataType.VarBinary, DbTypeInfos[Constants.DbTypes.VarBinary] },
			{ DataType.Blob, DbTypeInfos[Constants.DbTypes.Blob] },

			{ DataType.Char, DbTypeInfos[Constants.DbTypes.Char] },
			{ DataType.VarChar, DbTypeInfos[Constants.DbTypes.VarChar] },
			{ DataType.Text, DbTypeInfos[Constants.DbTypes.Clob] },

			{ DataType.NChar, DbTypeInfos[Constants.DbTypes.NChar] },
			{ DataType.NVarChar, DbTypeInfos[Constants.DbTypes.NVarChar] },
			{ DataType.NText, DbTypeInfos[Constants.DbTypes.NClob] },

			{ DataType.Boolean, DbTypeInfos[Constants.DbTypes.SmallInt] },
			{ DataType.Byte, DbTypeInfos[Constants.DbTypes.SmallInt] },
			{ DataType.SByte, DbTypeInfos[Constants.DbTypes.SmallInt] },
			{ DataType.Int16, DbTypeInfos[Constants.DbTypes.SmallInt] },

			{ DataType.UInt16, DbTypeInfos[Constants.DbTypes.Integer] },
			{ DataType.Int32, DbTypeInfos[Constants.DbTypes.Integer] },

			{ DataType.UInt32, DbTypeInfos[Constants.DbTypes.BigInt] },
			{ DataType.Int64, DbTypeInfos[Constants.DbTypes.BigInt] },

			{ DataType.UInt64, DbTypeInfos[Constants.DbTypes.Decimal] },
			{ DataType.Decimal, DbTypeInfos[Constants.DbTypes.Decimal] },

			{ DataType.Single, DbTypeInfos[Constants.DbTypes.Real] },
			{ DataType.Double, DbTypeInfos[Constants.DbTypes.Double] },

			{ DataType.DateTimeOffset, DbTypeInfos[Constants.DbTypes.TimeStamp] },
			{ DataType.Timestamp, DbTypeInfos[Constants.DbTypes.TimeStamp] },
			{ DataType.DateTime, DbTypeInfos[Constants.DbTypes.TimeStamp] },
			{ DataType.DateTime2, DbTypeInfos[Constants.DbTypes.TimeStamp] },

			{ DataType.Date, DbTypeInfos[Constants.DbTypes.Date] },
			{ DataType.Time, DbTypeInfos[Constants.DbTypes.Time] },

			{ DataType.Guid, DbTypeInfos[Constants.DbTypes.Char16ForBitData] },
		};

		public static DbDataType SanitizeDbDataType(DbDataType dbDataType, bool mapGuidAsString, bool supportsNCharTypes)
		{
			if (string.IsNullOrEmpty(dbDataType.DbType))
			{
				// Map special DataTypes
				dbDataType = dbDataType.DataType switch
				{
					DataType.UInt64 => dbDataType.WithDataType(DataType.Decimal).WithPrecisionScale(29, 0),
					DataType.Guid when mapGuidAsString => dbDataType.WithDataType(DataType.VarChar).WithLength(38),
					_ => dbDataType
				};

				// Look for DbTypeInfo
				if (DataTypeMap.TryGetValue(dbDataType.DataType, out var dbTypeInfo))
				{
					var ccsid = (int?)null;
					
					// Map N* Types to matching CCSID specific types if not supported
					if (!supportsNCharTypes)
					{
						if (dbTypeInfo.Name == Constants.DbTypes.NChar)
						{
							dbTypeInfo = DbGraphic;
							ccsid = 1200;
						}
						else if (dbTypeInfo.Name == Constants.DbTypes.NVarChar)
						{
							dbTypeInfo = DbVarGraphic;
							ccsid = 1200;
						}
						else if (dbTypeInfo.Name== Constants.DbTypes.NClob)
						{
							dbTypeInfo = DbClob;
							ccsid = 1200;
						}
					}

					// Sanitize Length, Precison and Scale based on DbTypeInfo definition
					static int? sanitize(int? parameter, int minValue, bool supported)
						=> !supported || parameter < minValue ? null : parameter;

					var length = sanitize(dbDataType.Length, 1, dbTypeInfo.HasLength);
					var precision = sanitize(dbDataType.Precision, 0, dbTypeInfo.HasPrecision);
					var scale = sanitize(dbDataType.Scale, 0, dbTypeInfo.HasScale);

					length ??= dbTypeInfo.RequiresLength ? dbTypeInfo.DefaultLength : null;
					precision ??= dbTypeInfo.RequiresPrecisionScale ? dbTypeInfo.DefaultPrecision : null;
					scale = precision.HasValue ? scale ?? (dbTypeInfo.RequiresPrecisionScale ? dbTypeInfo.DefaultScale : null) : null;

					dbDataType = dbDataType
						.WithLength(length)
						.WithPrecision(precision)
						.WithScale(scale);

					// If DbTypeInfo is composite, set the name as DbType hint
					if (dbTypeInfo.IsComposite)
					{
						dbDataType = dbDataType.WithDbType(dbTypeInfo.Name);
					}
					// Else build the DbType
					else
					{
						var fullTypeName = BuilFullDbTypeName(dbTypeInfo.Name, dbDataType.Length ?? dbDataType.Precision, dbDataType.Scale, ccsid, dbTypeInfo.ForBitData);
						dbDataType = dbDataType.WithDbType(fullTypeName);
					}
				}
			}
			//TODO: Check if sanitizing from an existing DbType is required

			return dbDataType;
		}

		public static DbDataType UpCastDbDataTypeToFit(DbDataType dbDataType, object? value)
		{
			var originalDbDataDatype = dbDataType;
			
			// Handle Length, Precision and Scale from the value itself if it exists
			if (value is not null)
			{
				switch (value)
				{
					case byte[] bytes:
						dbDataType = dbDataType.WithLength(bytes.Length);
						break;
					case string str:
						dbDataType = dbDataType.WithLength(str.Length);
						break;
					case decimal d:

						if (dbDataType.Precision == null)
						{
							var precision = PrecisionHelper.GetPrecision(d);
							var scale = PrecisionHelper.GetScale(d);
							if (precision == 0)
							{
								precision = 1;
								scale = 0;
							}
							else if (scale > precision) 
								scale = precision;

							if (precision > 0)
							{
								dbDataType = dbDataType.WithPrecision(precision);
								dbDataType = dbDataType.WithScale(scale);
							}
						}
						break;
				}
			}

			// Upcast required DataTypes
			switch (dbDataType.DataType)
			{
				case DataType.Char or DataType.VarChar
					when dbDataType.Length is null || dbDataType.Length > MAX_CHAR_LENGTH:
					dbDataType = dbDataType.WithDataType(DataType.Text);
					break;
				case DataType.NChar or DataType.NVarChar
					when dbDataType.Length is null || dbDataType.Length > MAX_NCHAR_LENGTH:
					dbDataType = dbDataType.WithDataType(DataType.NText);
					break;
				case DataType.Binary or DataType.VarBinary
					when dbDataType.Length is null || dbDataType.Length > MAX_CHAR_LENGTH:
					dbDataType = dbDataType.WithDataType(DataType.Blob);
					break;
				case DataType.Decimal:
					dbDataType = dbDataType.WithDataType(DataType.Decimal)
						.WithPrecisionScale(
							precision: dbDataType.Precision ?? 60, 
							scale: dbDataType.Precision.HasValue ? dbDataType.Scale ?? 30 : null);
					break;
				default: break;
			}

			// If there are any changes, clear the DbType hint to allow the sanitization to rebuild it properly before printing
			if (dbDataType != originalDbDataDatype
				&& !string.IsNullOrEmpty(dbDataType.DbType))
			{
				dbDataType = dbDataType.WithDbType(null);
			}

			return dbDataType;
		}

		//TODO: Check if acting on the DbType is required
		#region Decompose and Build DbType hint

		private static DbDataType GetDbDataTypeFromDbTypeName(DbDataType dbDataType, DbTypeName dbTypeName)
		{
			if (DbTypeInfos.TryGetValue(dbTypeName.TypeName, out var dbTypeInfo))
			{
				return new DbDataType(dbDataType.SystemType, dbTypeInfo.DataType, null, dbDataType.Length, dbDataType.Precision, dbDataType.Scale);
			}

			return dbDataType;
		}

		private static string BuilFullDbTypeName(string typeName, int? lengthOrPrecision, int? scale, int? ccsid, bool forBitData)
		{
			var stringBuilder = new StringBuilder(typeName);

			if (lengthOrPrecision.HasValue)
			{
				stringBuilder.Append('(').Append(lengthOrPrecision.Value.ToString(CultureInfo.InvariantCulture));
				if (scale.HasValue)
					stringBuilder.Append(", ") .Append(scale.Value.ToString(CultureInfo.InvariantCulture));
				stringBuilder.Append(')');
			}
			if (forBitData)
				stringBuilder.Append(" FOR BIT DATA");
			if (ccsid.HasValue)
			{
				stringBuilder.Append(" CCSID ").Append(ccsid.Value.ToString(CultureInfo.InvariantCulture));
			}

			return stringBuilder.ToString();
		}

		private static readonly Regex DbTypeRegex = new(
			@"^(?<type>\w+)" +
			@"(?:\((?<lengthOrPrecision>\d+)(?:,(?<scale>\d+))?\))?" +
			@"(?:\s+(?<bitData>FOR\s+BIT\s+DATA))?" +
			@"(?:\s+CCSID\s+(?<ccsid>\d+))?",
			RegexOptions.Compiled);

		private static bool TryParseDbTypeName(string dbType, out DbTypeName? dbTypeName)
		{
			dbTypeName = null;
			if (string.IsNullOrEmpty(dbType)) return false;
			dbType = dbType.ToUpper();

			var match = DbTypeRegex.Match(dbType);

			if (match.Success)
			{
				var type = dbType;
				int? lengthOrPrecision = null;
				int? scale = null;
				int? ccsid = null;

				if (match.Groups["type"].Success)
					type = match.Groups["type"].Value;
				if (match.Groups["lengthOrPrecision"].Success)
					lengthOrPrecision = int.Parse(match.Groups["lengthOrPrecision"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture);
				if (match.Groups["scale"].Success)
					scale = int.Parse(match.Groups["scale"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture);
				if (match.Groups["ccsid"].Success)
					ccsid = int.Parse(match.Groups["ccsid"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture);
				var forBitData = match.Groups["bitData"].Success;

				dbTypeName = new DbTypeName(dbType, type, lengthOrPrecision, scale, ccsid, forBitData);
				return true;
			}

			return false;
		}

		private readonly struct DbTypeName
		{
			public DbTypeName(string fullName, string typeName, int? lengthOrPecision, int? scale, int? cSSID, bool forBitData)
			{
				FullName = fullName;
				TypeName = typeName;
				LengthOrPrecision = lengthOrPecision;
				Scale = scale;
				CSSID = cSSID;
				ForBitData = forBitData;
			}

			public string FullName { get; }
			public string TypeName { get; }
			public int? LengthOrPrecision { get; }
			public int? Scale { get; }
			public int? CSSID { get; }
			public bool ForBitData { get; }
		}

		#endregion
	}
}
