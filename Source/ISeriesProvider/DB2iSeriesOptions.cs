namespace LinqToDB.DataProvider.DB2iSeries
{
	using Common;
	using Common.Internal;
	using Data;

	public sealed record DB2iSeriesOptions
	(
		BulkCopyType BulkCopyType = BulkCopyType.MultipleRows,
		DB2iSeriesIdentifierQuoteMode IdentifierQuoteMode = DB2iSeriesIdentifierQuoteMode.None
	)
		: DataProviderOptions<DB2iSeriesOptions>(BulkCopyType)
	{
		public DB2iSeriesOptions() : this(BulkCopyType.MultipleRows)
		{
		}

		DB2iSeriesOptions(DB2iSeriesOptions original) : base(original)
		{
			IdentifierQuoteMode = original.IdentifierQuoteMode;
		}

		protected override IdentifierBuilder CreateID(IdentifierBuilder builder) => builder
			.Add(IdentifierQuoteMode)
			;

		#region IEquatable implementation

		public bool Equals(DB2iSeriesOptions other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;

			return ((IOptionSet)this).ConfigurationID == ((IOptionSet)other).ConfigurationID;
		}

		public override int GetHashCode()
		{
			return ((IOptionSet)this).ConfigurationID;
		}

		#endregion
	}
}
