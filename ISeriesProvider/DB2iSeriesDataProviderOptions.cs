using System;
using System.Collections.Generic;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using LinqToDB.Configuration;

    //This class contains provider specific options and relevant logic
    //By implementing IEquatable it behaves like a value type in comparisons, but only for the properties that take part in Equals/GetHashCode
    //Currently ProviderType , MapGuidAsString and MinLevel do as they define the behaviour of the provider
    //IdentifierQuoteMode is simply an option that can be safely changed at runtime and thus doesn't

    public sealed class DB2iSeriesDataProviderOptions : IEquatable<DB2iSeriesDataProviderOptions>
    {
        public DB2iSeriesLevels MinLevel { get; }
        public bool MapGuidAsString { get; }
        public DB2iSeriesAdoProviderType AdoProviderType { get; }
        public DB2iSeriesIdentifierQuoteMode IdentifierQuoteMode { get; }
        public DB2iSeriesNamingConvention NamingConvention { get; }

        public DB2iSeriesDataProviderOptions(DB2iSeriesLevels minLevel, bool mapGuidAsString, 
            DB2iSeriesAdoProviderType dB2AdoProviderType, DB2iSeriesNamingConvention namingConvention,
            DB2iSeriesIdentifierQuoteMode identifierQuoteMode = DB2iSeriesIdentifierQuoteMode.None)
            :this(minLevel, mapGuidAsString, dB2AdoProviderType, identifierQuoteMode)
        {
            this.NamingConvention = dB2AdoProviderType == DB2iSeriesAdoProviderType.AccessClient ? DB2iSeriesNamingConvention.System : DB2iSeriesNamingConvention.Sql;
        }

        public DB2iSeriesDataProviderOptions(DB2iSeriesLevels minLevel, bool mapGuidAsString,
            DB2iSeriesAdoProviderType dB2AdoProviderType, 
            DB2iSeriesIdentifierQuoteMode identifierQuoteMode = DB2iSeriesIdentifierQuoteMode.None)
        {
            this.MinLevel = minLevel;
            this.MapGuidAsString = mapGuidAsString;
            this.AdoProviderType = dB2AdoProviderType;
            this.IdentifierQuoteMode = identifierQuoteMode;
        }

        public static DB2iSeriesDataProviderOptions FromAttributes(IEnumerable<NamedValue> attributes)
        {
            var attribs = attributes.ToList();

            var mapGuidAsString = false;
            var mapGuidAsStringAttribute = attribs.FirstOrDefault(_ => _.Name == nameof(MapGuidAsString));
            if (mapGuidAsStringAttribute != null)
                bool.TryParse(mapGuidAsStringAttribute.Value, out mapGuidAsString);


            var minLevel = DB2iSeriesLevels.Any;
            var minLevelAttribute = attribs.FirstOrDefault(_ => _.Name == nameof(MinLevel));
            if (minLevelAttribute != null)
                Enum.TryParse(minLevelAttribute.Value, out minLevel);

            var adoProviderType = DB2iSeriesAdoProviderType.AccessClient;
            var adoProviderTypeAttribute = attribs.FirstOrDefault(_ => _.Name == nameof(AdoProviderType));
            if (adoProviderTypeAttribute != null)
                Enum.TryParse(adoProviderTypeAttribute.Value, out adoProviderType);

            return new DB2iSeriesDataProviderOptions(minLevel, mapGuidAsString, adoProviderType);
        }

        public string GetProviderName()
        {
            return DB2iSeriesProviderName.GetFromOptions(this);
        }

        #region IEquitable

        private const int HashMultiplier = 37;

        public override int GetHashCode()
        {
            return new[]
            {
                AdoProviderType.GetHashCode(),
                MinLevel.GetHashCode(),
                MapGuidAsString.GetHashCode()
            }
            .Aggregate(HashMultiplier, (accumulate, item) =>
                (accumulate * HashMultiplier) ^ item);
        }

        public override bool Equals(object obj)
        {
            if (obj is DB2iSeriesDataProviderOptions other)
                return Equals(other);
            else
                return false;
        }

        public bool Equals(DB2iSeriesDataProviderOptions other)
        {
            return 
                AdoProviderType == other.AdoProviderType 
                && MinLevel == other.MinLevel
                && MapGuidAsString == other.MapGuidAsString;
        }

        public static bool operator ==(DB2iSeriesDataProviderOptions x, DB2iSeriesDataProviderOptions y)
        {
            if (x == null && y == null)
                return true;
            else if (x == null || y == null)
                return false;

            return x.Equals(y);
        }

        public static bool operator !=(DB2iSeriesDataProviderOptions x, DB2iSeriesDataProviderOptions y)
        {
            return !(x == y);
        }

        #endregion
    }
}