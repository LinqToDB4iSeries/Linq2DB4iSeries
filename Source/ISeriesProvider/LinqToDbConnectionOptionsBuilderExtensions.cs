using LinqToDB.DataProvider.DB2iSeries;
using System;

namespace LinqToDB.Configuration
{
	public static class LinqToDbConnectionOptionsBuilderExtensions
	{
		public static LinqToDBConnectionOptionsBuilder UseDB2iSeries(
			this LinqToDBConnectionOptionsBuilder builder, 
			string connectionString,
			Action<DB2iSeriesProviderConfigurationBuilder> configure = null)
		{
			var options = new DB2iSeriesProviderConfigurationBuilder();
			
			configure?.Invoke(options);

			return builder
				.UseConnectionString(
					DB2iSeriesTools.GetDataProvider(
						options.Version,
						options.ProviderType,
						options.MappingOptions), 
				connectionString);
		}
	}
}
