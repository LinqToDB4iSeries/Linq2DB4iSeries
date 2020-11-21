using System.Text;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.DB2iSeries;

#nullable disable

namespace Tests.DataProvider
{
	static class DB2iSeriesTestQueryExtensions
	{
		public static T ExecuteScalar<T>(this DataConnection connection, string value, string castTo = null)
			=> connection.Execute<T>(GetScalarQuery(value, castTo));


		public static T ExecuteScalarParameter<T>(this DataConnection connection, string parameterName, string parameterType, object parameterValue, DataType? dataType = null)
		{
			var parameter = new DataParameter(parameterName, parameterValue);

			return connection.ExecuteScalarParameter<T>(parameter, parameterType, dataType);
		}

		public static T ExecuteScalarParameter<T>(this DataConnection connection, DataParameter dataParameter, string parameterType, DataType? dataType = null)
		{
			if (connection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider
				&& (iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.Odbc
					|| iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.OleDb))
			{
				dataParameter.Name = "?";
			}

			if (dataType.HasValue)
				dataParameter.DataType = dataType.Value;

			return connection.Execute<T>(GetScalarParameterQuery(dataParameter.Name, parameterType), dataParameter);
		}

		public static T ExecuteScalarParameterObject<T>(this DataConnection connection, string parameterName, string parameterType, object parameterValuesObject)
		{
			if (connection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider
				&& (iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.Odbc
					|| iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.OleDb))
			{
				parameterName = "?";
			}

			return connection.Execute<T>(GetScalarParameterQuery(parameterName, parameterType), parameterValuesObject);
		}

		public static T ExecuteScalarParameterObject<T>(this DataConnection connection, string expression, object parameterValuesObject)
		{
			return connection.Execute<T>(GetScalarQuery(expression), parameterValuesObject);
		}

		private static string GetScalarQuery(string value, string castTo = null)
		{
			var sb = new StringBuilder().Append("SELECT ");
			if (!string.IsNullOrEmpty(castTo))
				sb.Append("CAST(");
			sb.Append(value);
			if (!string.IsNullOrEmpty(castTo))
				sb.Append(" AS ").Append(castTo).Append(")");
			sb.Append(" FROM SYSIBM.SYSDUMMY1");
			return sb.ToString();
		}

		private static string GetScalarParameterQuery(string parameterName, string parameterType)
		{
			var sb = new StringBuilder()
				.Append("SELECT ")
				.Append("CAST(")
				.Append(parameterName == "?" ? "" : "@").Append(parameterName)
				.Append(" AS ").Append(parameterType).Append(")")
				.Append("FROM SYSIBM.SYSDUMMY1");
			return sb.ToString();
		}

		public static string AsQuoted(this string s) => $"'{s}'";

		public static string GetParameterMarker(this DataConnection dataConnection, string parameterName, string castTo = null)
		{
			return GetValueSql(
				dataConnection.DataProvider is DB2iSeriesDataProvider iSeriesDataProvider
				&& (iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.Odbc
					|| iSeriesDataProvider.ProviderType == DB2iSeriesProviderType.OleDb)
				? "?" : parameterName, castTo);
		}

		public static string GetValueSql(string expression, string castTo = null)
		{
			var sb = new StringBuilder();
			if (!string.IsNullOrEmpty(castTo))
				sb.Append("CAST(");

			sb.Append(expression == "?" ? "" : "@").Append(expression);

			if (!string.IsNullOrEmpty(castTo))
				sb.Append(" AS ").Append(castTo).Append(")");
			return sb.ToString();
		}
	}
}
