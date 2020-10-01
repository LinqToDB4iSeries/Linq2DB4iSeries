﻿using System;
using System.Collections.Generic;
using System.Data;
using LinqToDB.Common;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Mapping;
	using SchemaProvider;
	using SqlProvider;
	using System.Threading;
	using System.Threading.Tasks;

	public class DB2iSeriesDataProvider : DynamicDataProviderBase<DB2iSeriesProviderAdapter>
	{
		private readonly DB2iSeriesLevels minLevel;
		private readonly bool mapGuidAsString;

		public DB2iSeriesDataProvider() : this(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false)
		{
		}

		public DB2iSeriesDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString)
			: base(
				  name,
				  GetMappingSchema(name, mapGuidAsString, DB2iSeriesProviderAdapter.GetInstance().MappingSchema),
				  DB2iSeriesProviderAdapter.GetInstance())
		{
			this.minLevel = minLevel;
			this.mapGuidAsString = mapGuidAsString;

			LoadExpressions(name, mapGuidAsString);

			SqlProviderFlags.AcceptsTakeAsParameter = false;
			SqlProviderFlags.AcceptsTakeAsParameterIfSkip = true;
			SqlProviderFlags.IsDistinctOrderBySupported = false;
			SqlProviderFlags.CanCombineParameters = false;
			SqlProviderFlags.IsParameterOrderDependent = true;
			SqlProviderFlags.IsCommonTableExpressionsSupported = true;
			
			if(mapGuidAsString)
				SqlProviderFlags.CustomFlags.Add(Constants.ProviderFlags.MapGuidAsString);

			SetCharField(Constants.DbTypes.Char, (r, i) => r.GetString(i).TrimEnd(' '));
			SetCharField(Constants.DbTypes.NChar, (r, i) => r.GetString(i).TrimEnd(' '));
			
			_sqlOptimizer = new DB2iSeriesSqlOptimizer(SqlProviderFlags);
			
			SetProviderField(Adapter.iDB2BigIntType, typeof(long), Adapter.GetiDB2BigIntReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2BinaryType, typeof(byte[]), Adapter.GetiDB2BinaryReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2BlobType, typeof(byte[]), Adapter.GetiDB2BlobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2CharType, typeof(string), Adapter.GetiDB2CharReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2CharBitDataType, typeof(byte[]), Adapter.GetiDB2CharBitDataReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2ClobType, typeof(string), Adapter.GetiDB2ClobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DataLinkType, typeof(string), Adapter.GetiDB2DataLinkReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DateType, typeof(DateTime), Adapter.GetiDB2DateReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DbClobType, typeof(string), Adapter.GetiDB2DbClobReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DecFloat16Type, typeof(decimal), Adapter.GetiDB2DecFloat16ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DecFloat34Type, typeof(decimal), Adapter.GetiDB2DecFloat34ReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DecimalType, typeof(decimal), Adapter.GetiDB2DecimalReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2DoubleType, typeof(double), Adapter.GetiDB2DoubleReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2GraphicType, typeof(string), Adapter.GetiDB2GraphicReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2IntegerType, typeof(int), Adapter.GetiDB2IntegerReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2NumericType, typeof(decimal), Adapter.GetiDB2NumericReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2RealType, typeof(float), Adapter.GetiDB2RealReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2RowidType, typeof(byte[]), Adapter.GetiDB2RowidReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2SmallIntType, typeof(short), Adapter.GetiDB2SmallIntReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2TimeType, typeof(DateTime), Adapter.GetiDB2TimeReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2TimeStampType, typeof(DateTime), Adapter.GetiDB2TimeStampReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarBinaryType, typeof(byte[]), Adapter.GetiDB2VarBinaryReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarCharType, typeof(string), Adapter.GetiDB2VarCharReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarCharBitDataType, typeof(byte[]), Adapter.GetiDB2VarCharBitDataReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2VarGraphicType, typeof(string), Adapter.GetiDB2VarGraphicReaderMethod, dataReaderType: Adapter.DataReaderType);
			SetProviderField(Adapter.iDB2XmlType, typeof(string), Adapter.GetiDB2XmlReaderMethod, dataReaderType: Adapter.DataReaderType);
		}

		
		readonly DB2iSeriesSqlOptimizer _sqlOptimizer;
		
		public override ISqlBuilder CreateSqlBuilder(MappingSchema mappingSchema)
		{
			return minLevel == DB2iSeriesLevels.V7_1_38 ?
				new DB2iSeriesSqlBuilder7_2(this, mappingSchema, GetSqlOptimizer(), SqlProviderFlags) :
				new DB2iSeriesSqlBuilder(this, mappingSchema, GetSqlOptimizer(), SqlProviderFlags);
		}

		public override ISchemaProvider GetSchemaProvider()
		{
			return new DB2iSeriesSchemaProvider(this);
		}

		public override ISqlOptimizer GetSqlOptimizer()
		{
			return _sqlOptimizer;
		}

		public override void InitCommand(DataConnection dataConnection, CommandType commandType, string commandText, DataParameter[] parameters, bool withParameters)
		{
			dataConnection.DisposeCommand();

			base.InitCommand(dataConnection, commandType, commandText, parameters, withParameters);
		}

		private static MappingSchema GetMappingSchema(string configuration, bool mapGuidAsString, MappingSchema providerSchema)
		{
			return mapGuidAsString
				? (MappingSchema)new DB2iSeriesGuidAsStringMappingSchema(configuration, providerSchema)
				: new DB2iSeriesMappingSchema(configuration, providerSchema);
		}

		public override void SetParameter(DataConnection dataConnection, IDbDataParameter parameter, string name, DbDataType dataType, object value)
		{
			if (value is sbyte @sbyte)
			{
				value = (short)@sbyte;
				dataType = dataType.WithDataType(DataType.Int16);
			}
			else if (value is byte @byte)
			{
				value = (short)@byte;
				dataType = dataType.WithDataType(DataType.Int16);
			}

			switch (dataType.DataType)
			{
				case DataType.UInt16:
					dataType = dataType.WithDataType(DataType.Int32);
					if (value != null)
						value = Convert.ToInt32(value);
					break;
				case DataType.UInt32:
					dataType = dataType.WithDataType(DataType.Int64);
					if (value != null)
						value = Convert.ToInt64(value);
					break;
				case DataType.UInt64:
					dataType = dataType.WithDataType(DataType.Decimal);
					if (value != null)
						value = Convert.ToDecimal(value);
					break;
				case DataType.VarNumeric: dataType = dataType.WithDataType(DataType.Decimal); break;
				case DataType.DateTime2: dataType = dataType.WithDataType(DataType.DateTime); break;
				case DataType.Char:
				case DataType.VarChar:
				case DataType.NChar:
				case DataType.NVarChar:
				case DataType.Text:
				case DataType.NText:
					if (value is Guid textGuid) value = textGuid.ToString("D");
					else if (value is bool textBool)
						value = ConvertTo<char>.From(textBool);
					break;
				case DataType.Boolean:
				case DataType.Int16:
					if (value is bool boolean)
					{
						value = boolean ? 1 : 0;
						dataType = dataType.WithDataType(DataType.Int16);
					}
					break;
				case DataType.Guid:
					dataType = dataType.WithDataType(
						mapGuidAsString ? DataType.NVarChar : DataType.VarBinary);

					if (value is Guid guid)
						value = mapGuidAsString ? 
							(object)guid.ToString("D") : guid.ToByteArray();
						
					break;
				case DataType.Binary:
				case DataType.VarBinary:
					if (value is Guid varBinaryGuid) value = varBinaryGuid.ToByteArray();
					else if (parameter.Size == 0 && value != null && 
						(value.GetType() == Adapter.iDB2BinaryType 
						|| value.GetType() == Adapter.iDB2BinaryType))
					{
						dynamic v = value;
						if (v.IsNull)
							value = DBNull.Value;
					}
					break;
				case DataType.Time:
					//Time parameters will only accept iDb2Time or string representation of time
					if (value is TimeSpan timeSpan)
					{
						value = DB2iSeriesSqlBuilder.ConvertTimeToSql(timeSpan, false);
					}
					else if (value is DateTime dateTime)
					{
						value = DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Time, dateTime, false);
					}
					else if (value is DateTimeOffset dateTimeOffset)
					{
						value = DB2iSeriesSqlBuilder.ConvertDateTimeToSql(DataType.Time, dateTimeOffset.DateTime, false);
					}
					break;
			}

			base.SetParameter(dataConnection, parameter, "@" + name, dataType, value);
		}

		protected override void SetParameterType(DataConnection dataConnection, IDbDataParameter parameter, DbDataType dataType)
		{
			DB2iSeriesProviderAdapter.iDB2DbType? type = null;
			switch (dataType.DataType)
			{
				case DataType.Blob: type = DB2iSeriesProviderAdapter.iDB2DbType.iDB2Blob; break;
			}

			if (type != null)
			{
				var param = TryGetProviderParameter(parameter, dataConnection.MappingSchema);
				if (param != null)
				{
					Adapter.SetDbType(param, type.Value);
					return;
				}
			}

			base.SetParameterType(dataConnection, parameter, dataType);
		}

		#region BulkCopy

		DB2iSeriesBulkCopy _bulkCopy;

		public override BulkCopyRowsCopied BulkCopy<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source)
		{
			if (_bulkCopy == null)
				_bulkCopy = new DB2iSeriesBulkCopy();

			return _bulkCopy.BulkCopy(
			  options.BulkCopyType == BulkCopyType.Default ? DB2iSeriesTools.DefaultBulkCopyType : options.BulkCopyType,
			  table,
			  options,
			  source);
		}

		public override Task<BulkCopyRowsCopied> BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (_bulkCopy == null)
				_bulkCopy = new DB2iSeriesBulkCopy();

			return _bulkCopy.BulkCopyAsync(
			  options.BulkCopyType == BulkCopyType.Default ? DB2iSeriesTools.DefaultBulkCopyType : options.BulkCopyType,
			  table,
			  options,
			  source,
			  cancellationToken);
		}

#if !NETFRAMEWORK
		public override BulkCopyRowsCopied BulkCopyAsync<T>(ITable<T> table, BulkCopyOptions options, IAsyncEnumerable<T> source, CancellationToken cancellationToken)
		{
			if (_bulkCopy == null)
				_bulkCopy = new DB2iSeriesBulkCopy();

			return _bulkCopy.BulkCopyAsync(
			  options.BulkCopyType == BulkCopyType.Default ? DB2iSeriesTools.DefaultBulkCopyType : options.BulkCopyType,
			  table,
			  options,
			  source,
			  cancellationToken);
		}
#endif
		#endregion

		#region Expressions

		private static void LoadExpressions(string providerName, bool mapGuidAsString)
		{
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Space(0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Int32?, String>(p0 => Sql.Convert(Sql.VarChar(1000), Linq.Expressions.Replicate(" ", p0)))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Stuff("", 0, 0, "")),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Int32?, String, String>((p0, p1, p2, p3) => Linq.Expressions.AltStuff(p0, p1, p2, p3))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.PadRight("", 0, ' ')),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Char?, String>((p0, p1, p2) => p0.Length > p1 ? p0 : p0 + Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.PadLeft("", 0, ' ')),
				Linq.Expressions.N(() => Linq.Expressions.L<String, Int32?, Char?, String>((p0, p1, p2) => p0.Length > p1 ? p0 : Linq.Expressions.VarChar(Linq.Expressions.Replicate(p2, p1 - p0.Length), 1000) + p0)));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.ConvertTo<String>.From((Decimal)0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Decimal, String>((Decimal p) => Sql.TrimLeft(Sql.Convert<string, Decimal>(p), '0'))));

			if (!mapGuidAsString)
			{
				Linq.Expressions.MapMember(
					providerName,
					Linq.Expressions.M(() => Sql.ConvertTo<String>.From(Guid.Empty)),
					Linq.Expressions.N(() => Linq.Expressions.L<Guid, String>(
						(Guid p) => Sql.Lower(Sql.Substring(Linq.Expressions.Hex(p), 7, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 5, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 3, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 1, 2)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 11, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 9, 2)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 15, 2)
											  + Sql.Substring(Linq.Expressions.Hex(p), 13, 2)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 17, 4)
											  + "-" + Sql.Substring(Linq.Expressions.Hex(p), 21, 12)))));
			}

			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Log(0m, 0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Decimal?, Decimal?, Decimal?>((m, n) => Sql.Log(n) / Sql.Log(m))));
			Linq.Expressions.MapMember(
				providerName,
				Linq.Expressions.M(() => Sql.Log(0.0, 0)),
				Linq.Expressions.N(() => Linq.Expressions.L<Double?, Double?, Double?>((m, n) => Sql.Log(n) / Sql.Log(m))));
		}

		#endregion
	}
}
