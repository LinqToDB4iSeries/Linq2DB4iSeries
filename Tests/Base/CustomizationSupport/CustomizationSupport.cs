namespace Tests
{
	public static class CustomizationSupport
	{
		public static readonly CustomizationSupportInterceptor Interceptor = new Db2iInterceptor();
	}
}
