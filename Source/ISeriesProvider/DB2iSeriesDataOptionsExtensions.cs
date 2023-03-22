using LinqToDB.DataProvider.DB2;
using LinqToDB.DataProvider.DB2iSeries;
using System;

namespace LinqToDB
{
	public static class DB2iSeriesDataOptionsExtensions
	{
		/// <summary>
		/// Configure connection to use DB2iSeries default provider and connection string.
		/// </summary>
		/// <param name="options">Instance of <see cref="DataOptions"/>.</param>
		/// <param name="connectionString">DB2iSeries connection string.</param>
		/// <param name="optionSetter">Optional <see cref="DB2Options"/> configuration callback.</param>
		/// <returns>The builder instance so calls can be chained.</returns>
		/// <remarks>
		/// <para>
		/// DB2iSeries provider will be chosen automatically:
		/// <list type="bullet">
		/// <item>ADO provider type will detected form connection string.</item>
		/// <item>if <see cref="DB2Tools.AutoDetectProvider"/> (default: <c>true</c>) enabled, LinqToDB will query server for version</item>
		/// <item>otherwise v7.1 will be chosen.</item>
		/// </list>
		/// </para>
		/// For more fine-grained configuration see <see cref="UseDB2(DataOptions, string, DB2Version, Func{DB2Options, DB2Options}?)"/> overload.
		/// </remarks>
		public static DataOptions UseDB2iSeries(this DataOptions options, string connectionString,
			Func<DB2iSeriesOptions, DB2iSeriesOptions> optionSetter = null)
		{
			options = options.UseConnectionString(DB2iSeriesProviderName.DB2, connectionString);
			return optionSetter != null ? options.WithOptions(optionSetter) : options;
		}

		/// <summary>
		/// Configure connection to use specific DB2iSeries provider.
		/// </summary>
		/// <param name="options">Instance of <see cref="DataOptions"/>.</param>
		/// <param name="providerType">DB2iSeries ADO provider type.</param>
		/// <param name="version">DB2iSeries server version.</param>
		/// <param name="mappingOptions">DB2iSeries mapping options.</param>/// 
		/// <param name="optionSetter">Optional <see cref="DB2iSeriesOptions"/> configuration callback.</param>
		/// <returns>The builder instance so calls can be chained.</returns>
		public static DataOptions UseDB2iSeries(this DataOptions options,
			DB2iSeriesProviderType providerType = DB2iSeriesProviderOptions.Defaults.ProviderType,
			DB2iSeriesVersion version = DB2iSeriesProviderOptions.Defaults.Version,
			DB2iSeriesMappingOptions mappingOptions = null,
			Func<DB2iSeriesOptions, DB2iSeriesOptions> optionSetter = null)
		{
			mappingOptions ??= DB2iSeriesMappingOptions.Default;
			options = options.UseDataProvider(DB2iSeriesTools.GetDataProvider(version, providerType, mappingOptions));
			return optionSetter != null ? options.WithOptions(optionSetter) : options;
		}

		/// <summary>
		/// Configure connection to use specific DB2iSeries provider and connection string.
		/// </summary>
		/// <param name="options">Instance of <see cref="DataOptions"/>.</param>
		/// <param name="connectionString">DB2 connection string.</param>
		/// <param name="providerType">DB2iSeries ADO provider type.</param>
		/// <param name="version">DB2iSeries server version.</param>
		/// <param name="mappingOptions">DB2iSeries mapping options.</param>/// 
		/// <param name="optionSetter">Optional <see cref="DB2iSeriesOptions"/> configuration callback.</param>
		/// <returns>The builder instance so calls can be chained.</returns>
		public static DataOptions UseDB2iSeries(this DataOptions options,
			string connectionString,
			DB2iSeriesProviderType providerType,
			DB2iSeriesVersion version = DB2iSeriesProviderOptions.Defaults.Version,
			DB2iSeriesMappingOptions mappingOptions = null,
			Func<DB2iSeriesOptions, DB2iSeriesOptions> optionSetter = null)
		{
			mappingOptions ??= DB2iSeriesMappingOptions.Default;
			options = options.UseConnectionString(DB2iSeriesTools.GetDataProvider(version, providerType, mappingOptions), connectionString);
			return optionSetter != null ? options.WithOptions(optionSetter) : options;
		}
	}
}
