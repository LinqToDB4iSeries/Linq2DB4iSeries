using System;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace IBM.Data.Informix
{
	public class IfxTimeSpan { }
}

namespace System.Runtime.CompilerServices
{
	public static class IsExternalInit { }
}


namespace JetBrains.Annotations
{
	[AttributeUsage(AttributeTargets.Parameter)]
	public sealed class NoEnumerationAttribute : Attribute
	{
		
	}
}

#if NETFRAMEWORK
namespace System.Diagnostics.CodeAnalysis
{
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field | AttributeTargets.Parameter | AttributeTargets.ReturnValue, Inherited = false)]
	public sealed class MaybeNullAttribute : Attribute
	{
	}

	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field | AttributeTargets.Parameter, Inherited = false)]
	public sealed class AllowNullAttribute : Attribute
	{
	}

	public sealed class NotNullIfNotNullAttribute : Attribute
	{
		public string ParameterName { get; }
		
		public NotNullIfNotNullAttribute(string parameterName)
		{
			ParameterName = parameterName;
		}
	}
}
#endif

namespace Tests
{
	public class SapHanaTests
	{
		public class FIT_CA_PARAM_TEST { }
		public class CalcViewInputParameters : LinqToDB.DataContext
		{
			public CalcViewInputParameters(string context)
			{

			}

			public IQueryable<FIT_CA_PARAM_TEST> CaParamTest(int v, object? p1, string? var1, object? p2, object? p3, object? p4)
			{
				throw new NotImplementedException();
			}
		}
	}

}
