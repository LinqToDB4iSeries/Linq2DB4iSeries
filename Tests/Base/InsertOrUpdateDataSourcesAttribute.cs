using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests
{
	public class InsertOrUpdateDataSourcesAttribute : DataSourcesAttribute
	{
		static string[] Unsupported = 
			Split(new string[]
			{
				
			})
			.ToArray();

		public InsertOrUpdateDataSourcesAttribute(params string[] except)
			: base(true, Unsupported.Concat(Split(except)).ToArray())
		{
		}

		public InsertOrUpdateDataSourcesAttribute(bool includeLinqService, params string[] except)
			: base(includeLinqService, Unsupported.Concat(Split(except)).ToArray())
		{
		}
	}
}
