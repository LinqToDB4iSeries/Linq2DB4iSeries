﻿using LinqToDB;
using LinqToDB.Mapping;

namespace Tests.Model
{
	[Table ("Person",     IsColumnAttributeRequired = false)]
	[Column("FirstName",  "Name.FirstName")]
	[Column("MiddleName", "Name.MiddleName")]
	[Column("LastName",   "Name.LastName")]
	public class ComplexPerson : IPerson
	{

		[SequenceName(ProviderName.Firebird, "PersonID")]
		[Column("PersonID", Configuration = ProviderName.ClickHouse)]
		[Column("PersonID", IsIdentity = true)]
		[PrimaryKey]
		public int      ID     { get; set; }
		public Gender   Gender { get; set; }
		public FullName Name   { get; set; } = null!;

		[NotColumn]
		int IPerson.ID
		{
			get { return ID; }
			set { ID = value; }
		}

		[NotColumn]
		Gender IPerson.Gender
		{
			get { return Gender; }
			set { Gender = value; }
		}

		[NotColumn]
		string IPerson.FirstName
		{
			get { return Name.FirstName; }
			set { Name.FirstName = value; }
		}

		[NotColumn]
		string? IPerson.MiddleName
		{
			get { return Name.MiddleName; }
			set { Name.MiddleName = value; }
		}

		[NotColumn]
		string IPerson.LastName
		{
			get { return Name.LastName; }
			set { Name.LastName = value; }
		}
	}

	[Table ("Person",     IsColumnAttributeRequired = false)]
	[Column("FirstName",  "Name.FirstName")]
	[Column("MiddleName", "Name.MiddleName")]
	[Column("LastName",   "Name.LastName")]
	public class ComplexPerson2 
	{

		[PrimaryKey]
		[SequenceName(ProviderName.Firebird, "PersonID")]
		[Column("PersonID", Configuration = ProviderName.ClickHouse)]
		[Column("PersonID", IsIdentity = true)]
		public int      ID     { get; set; }
		public Gender   Gender { get; set; }
		public FullName Name   { get; set; } = null!;
	}

	public class ComplexPerson3 
	{
		public int      ID     { get; set; }
		public Gender   Gender { get; set; }
		public FullName Name   { get; set; } = null!;
	}
}
