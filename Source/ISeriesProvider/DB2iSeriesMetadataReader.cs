using System;
using System.Linq;
using System.Reflection;

using static LinqToDB.Sql;

using LinqToDB.Metadata;
using LinqToDB.Mapping;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal partial class DB2iSeriesMetadataReader : IMetadataReader
	{
		private readonly string providerName;
		private readonly MappingAttributes attributes;

		public DB2iSeriesMetadataReader(string providerName)
		{
			this.providerName = providerName;
			this.attributes = new(providerName);
		}

		public MappingAttribute[] GetAttributes(Type type, MemberInfo memberInfo)
		{
			var attribute = GetMappingAttribute(type, memberInfo);
			return attribute is not null ? [attribute] : [];
		}

		private MappingAttribute? GetMappingAttribute(Type type, MemberInfo memberInfo)
		{
			if (type == typeof(Sql))
			{
				return memberInfo.Name switch
				{
					nameof(Sql.ZeroPad) => attributes.ZeroPad,
					nameof(Sql.CharIndex) => attributes.CharIndex,
					nameof(Sql.Substring) => attributes.Substring,
					nameof(Sql.TrimLeft) => attributes.Trim,
					nameof(Sql.TrimRight) => attributes.Trim,
					nameof(Sql.Truncate) => attributes.Truncate,
					nameof(Sql.DateDiff) => attributes.DateDiff,
					nameof(Sql.Log) => attributes.Log,
					nameof(Sql.Log10) => attributes.Log10,
					nameof(Sql.Atan2) => attributes.Atan2,
					nameof(Sql.StringAggregate) when memberInfo is MethodInfo stringAggregateMethod
						=> stringAggregateMethod.GetParameters().Any(x => x.Name == "selector") ? attributes.StringAggregateSelector : attributes.StringAggregateSource,
					nameof(Sql.CurrentTimestampUtc) => attributes.CurrentTimestampUtc,
					_ => null
				};
			}

			if (type == typeof(Types))
			{
				return memberInfo.Name switch
				{
					nameof(Types.TinyInt) => attributes.TinyInt,
					nameof(Types.DefaultNChar) => attributes.DefaultGraphic,
					nameof(Types.DefaultNVarChar) => attributes.DefaultGraphic,
					nameof(Types.NChar) => attributes.Graphic,
					nameof(Types.NVarChar) => attributes.Graphic,
					_ => null
				};
			}

			if (type == typeof(Linq.Expressions))
			{
				return memberInfo.Name switch
				{
					nameof(Linq.Expressions.TrimLeft) => attributes.Trim,
					nameof(Linq.Expressions.TrimRight) => attributes.Trim,
					nameof(Linq.Expressions.Replicate) => attributes.Replicate,
					_ => null
				};
			}

			return null;
		}

		public MappingAttribute[] GetAttributes(Type type)
		{
			return [];
		}

		public string GetObjectID()
		{
			return providerName;
		}

		public MemberInfo[] GetDynamicColumns(Type type)
		{
			return [];
		}
	}
}
