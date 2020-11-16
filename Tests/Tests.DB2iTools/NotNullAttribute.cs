#nullable disable

#if NET472
namespace System.Diagnostics.CodeAnalysis
{
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter | AttributeTargets.ReturnValue, AllowMultiple = true)]
	public sealed class NotNullAttribute : Attribute
	{
		public string ParameterName { get; }

		public NotNullAttribute()
		{

		}

		public NotNullAttribute(string parameterName)
		{
			ParameterName = parameterName;
		}
	}
}
#endif
