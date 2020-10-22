namespace LinqToDB.DataProvider.DB2iSeries
{
	internal struct ServerVersion
	{
		public ServerVersion(int major, int minor, int patchLevel)
		{
			Major = major;
			Minor = minor;
			PatchLevel = patchLevel;
		}

		public int Major { get; }
		public int Minor { get; }
		public int PatchLevel { get; }

		public override int GetHashCode()
		{
			return Major.GetHashCode() + Minor.GetHashCode() + PatchLevel.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			if (obj is ServerVersion other)
				return other.Major == Major && other.Minor == Minor && other.PatchLevel == PatchLevel;

			return false;
		}

		public static bool operator ==(ServerVersion a, ServerVersion b)
		{
			return a.Equals(b);
		}

		public static bool operator !=(ServerVersion a, ServerVersion b)
		{
			return !a.Equals(b);
		}

		public static bool operator >(ServerVersion a, ServerVersion b)
		{
			return a.Major > b.Major ||
				a.Major == b.Major && a.Minor > b.Minor ||
				a.Major == b.Major && a.Minor == b.Minor && a.PatchLevel > b.PatchLevel;
		}

		public static bool operator >=(ServerVersion a, ServerVersion b)
		{
			return a == b || a > b;
		}

		public static bool operator <(ServerVersion a, ServerVersion b)
		{
			return a.Major < b.Major ||
				a.Major == b.Major && a.Minor < b.Minor ||
				a.Major == b.Major && a.Minor == b.Minor && a.PatchLevel < b.PatchLevel;
		}

		public static bool operator <=(ServerVersion a, ServerVersion b)
		{
			return a == b || a < b;
		}
	}
}
