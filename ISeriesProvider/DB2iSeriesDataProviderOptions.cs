using System;
using System.Collections.Generic;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using LinqToDB.Configuration;

    public sealed class DB2iSeriesDataProviderOptions : IEquatable<DB2iSeriesDataProviderOptions>
    {
        public DB2iSeriesLevels MinLevel { get; }
        public bool MapGuidAsString { get; }
        public DB2iSeriesAdoProviderType AdoProviderType { get; }

        public DB2iSeriesDataProviderOptions(DB2iSeriesLevels minLevel, bool mapGuidAsString, DB2iSeriesAdoProviderType dB2AdoProviderType)
        {
            this.MinLevel = minLevel;
            this.MapGuidAsString = mapGuidAsString;
            this.AdoProviderType = dB2AdoProviderType;
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