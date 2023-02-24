using static LinqToDB.Sql;

namespace LinqToDB.DataProvider.DB2iSeries
{
	internal partial class DB2iSeriesMetadataReader
	{
		private sealed class MappingAttributes
		{
			public MappingAttributes(string providerName)
			{
				ZeroPad = new ExpressionAttribute(providerName, "Lpad({0}, {1}, '0')") { IsNullable = IsNullableType.SameAsFirstParameter };
				Substring = new FunctionAttribute(providerName, "Substr") { PreferServerSide = true, IsNullable = IsNullableType.IfAnyParameterNullable };
				CharIndex = new FunctionAttribute(providerName, "Locate") { IsNullable = IsNullableType.IfAnyParameterNullable };
				Trim = new ExtensionAttribute(providerName, "") { ServerSideOnly = false, PreferServerSide = false, BuilderType = typeof(TrimBuilderDB2i) };
				Truncate = new ExpressionAttribute(providerName, "Truncate({0}, 0)") { IsNullable = IsNullableType.IfAnyParameterNullable };
				StringAggregateSource = new ExtensionAttribute(providerName, "LISTAGG({source}, {separator}){_}{aggregation_ordering?}") { IsAggregate = true, ChainPrecedence = 10 };
				StringAggregateSelector = new ExtensionAttribute(providerName, "LISTAGG({selector}, {separator}){_}{aggregation_ordering?}") { IsAggregate = true, ChainPrecedence = 10 };
				Replicate = new FunctionAttribute(providerName, "Repeat") { IsNullable = IsNullableType.IfAnyParameterNullable };
				Log = new FunctionAttribute(providerName, "Ln") { IsNullable = IsNullableType.IfAnyParameterNullable };
				Log10 = new FunctionAttribute(providerName, "Log10") { IsNullable = IsNullableType.IfAnyParameterNullable };
				Atan2 = new FunctionAttribute(providerName, "Atan2", 1, 0) { IsNullable = IsNullableType.IfAnyParameterNullable };
				DateAdd = new ExtensionAttribute(providerName, "") { ServerSideOnly = false, PreferServerSide = false, BuilderType = typeof(DateAddBuilderDB2i) };
				DatePart = new ExtensionAttribute(providerName, "") { ServerSideOnly = false, PreferServerSide = false, BuilderType = typeof(DatePartBuilderDB2i) };
				DateDiff = new ExtensionAttribute(providerName, "") { BuilderType = typeof(DateDiffBuilderDB2i) };
				TinyInt = new PropertyAttribute(providerName, "SmallInt") { ServerSideOnly = true, CanBeNull = false };
				DefaultGraphic = new PropertyAttribute(providerName, "Graphic") { ServerSideOnly = true, CanBeNull = false };
				Graphic = new FunctionAttribute(providerName, "Graphic") { ServerSideOnly = true, CanBeNull = false };
			}

			public FunctionAttribute CharIndex { get; }
			public ExtensionAttribute Trim { get; }
			public ExpressionAttribute Truncate { get; }
			public ExtensionAttribute StringAggregateSource { get; }
			public ExtensionAttribute StringAggregateSelector { get; }
			public FunctionAttribute Replicate { get; }
			public FunctionAttribute Log { get; }
			public FunctionAttribute Log10 { get; }
			public FunctionAttribute Atan2 { get; }
			public ExtensionAttribute DateAdd { get; }
			public ExtensionAttribute DatePart { get; }
			public ExtensionAttribute DateDiff { get; }
			public PropertyAttribute TinyInt { get; }
			public PropertyAttribute DefaultGraphic { get; }
			public FunctionAttribute Graphic { get; }
			public ExpressionAttribute ZeroPad { get; }
			public FunctionAttribute Substring { get; }
		}
	}
}
