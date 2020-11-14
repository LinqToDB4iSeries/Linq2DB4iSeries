using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests
{
	public class InsertOrUpdateDataSourcesAttribute : DataSourcesAttribute
	{
		static string[] Unsupported = new[]
		{
				TestProvNameDb2i.All_54,
			}.SelectMany(_ => _.Split(',')).ToArray();

		public InsertOrUpdateDataSourcesAttribute(params string[] except)
			: base(true, Unsupported.Concat(except.SelectMany(_ => _.Split(','))).ToArray())
		{
		}

		public InsertOrUpdateDataSourcesAttribute(bool includeLinqService, params string[] except)
			: base(includeLinqService, Unsupported.Concat(except.SelectMany(_ => _.Split(','))).ToArray())
		{
		}
	}
}
