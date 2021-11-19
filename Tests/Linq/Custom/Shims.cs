using System;
using System.Linq;

namespace IBM.Data.Informix
{
	public class IfxTimeSpan { }
	
}

namespace Tests
{
	public class SapHanaTests
	{
		public class FIT_CA_PARAM_TEST { }
		public class CalcViewInputParameters : LinqToDB.DataContext
		{
			public CalcViewInputParameters(string context)
			{

			}

			internal IQueryable<FIT_CA_PARAM_TEST> CaParamTest(int v, object p1, string var1, object p2, object p3, object p4)
			{
				throw new NotImplementedException();
			}
		}
	}
}
