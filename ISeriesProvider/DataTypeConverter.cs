using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal static class DataTypeConverter
	{
		static readonly Dictionary<DataType, Type> typeMap = new Dictionary<DataType, Type> {
			{ DataType.Int16, typeof(short) },
			{ DataType.Int32, typeof(int) },
			{ DataType.Int64, typeof(long) },
			{ DataType.Decimal, typeof(decimal) },
			{ DataType.Single, typeof(float) },
			{ DataType.Double, typeof(double) }
		};

		static readonly Dictionary<DataType, TypeConverter> dataTypeConverters =
			typeMap.ToDictionary(x => x.Key, x => TypeDescriptor.GetConverter(x.Value));

		public static bool TryConvert(object value, DataType dataType, out object result)
		{
			result = value;
			if (value == null)
				return true;

			var sourceType = value.GetType();

			if (dataTypeConverters.TryGetValue(dataType, out var converter)
				&& converter.CanConvertFrom(sourceType))
			{
				result = converter.ConvertFrom(value);
				return true;
			}

			if (typeMap.TryGetValue(dataType, out var type))
			{
				if (TryConvert(value, type, out result))
					return true;

				converter = TypeDescriptor.GetConverter(sourceType);
				if (converter.CanConvertTo(type))
				{
					result = converter.ConvertTo(value, type);
					return true;
				}
			}

			return false;
		}

		public static bool TryConvert(object value, Type type, out object result)
		{
			result = value;

			if (value is IConvertible c)
			{
				var formatProvider = System.Globalization.CultureInfo.CurrentCulture;

				if (type == typeof(byte))
				{
					result = c.ToByte(formatProvider); return true;
				}
				if (type == typeof(sbyte))
				{
					result = c.ToSByte(formatProvider); return true;
				}
				if (type == typeof(short))
				{
					result = c.ToInt16(formatProvider); return true;
				}
				if (type == typeof(ushort))
				{
					result = c.ToUInt16(formatProvider); return true;
				}
				else if (type == typeof(int))
				{
					result = c.ToInt32(formatProvider); return true;
				}
				else if (type == typeof(uint))
				{
					result = c.ToUInt32(formatProvider); return true;
				}
				else if (type == typeof(long))
				{
					result = c.ToInt64(formatProvider); return true;
				}
				else if (type == typeof(ulong))
				{
					result = c.ToUInt64(formatProvider); return true;
				}
				else if (type == typeof(decimal))
				{
					result = c.ToDecimal(formatProvider); return true;
				}
				else if (type == typeof(float))
				{
					result = c.ToSingle(formatProvider); return true;
				}
				else if (type == typeof(double))
				{
					result = c.ToDouble(formatProvider); return true;
				}
				else if (type == typeof(string))
				{
					result = c.ToString(formatProvider); return true;
				}
				else if (type == typeof(bool))
				{
					result = c.ToBoolean(formatProvider); return true;
				}
				else if (type == typeof(DateTime))
				{
					result = c.ToDateTime(formatProvider); return true;
				}
			}

			return false;
		}

		public static object TryConvertOrOriginal(object value, DataType dataType, out bool converted)
		{
			converted = TryConvert(value, dataType, out var v);
			return v;
		}

		public static object TryConvertOrOriginal(object value, DataType dataType)
		{
			TryConvert(value, dataType, out var v);
			return v;
		}
	}
}
