using LinqToDB.Extensions;
using System;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public abstract class TypeCreatorBase
	{
		public Type Type;

        public TypeCreatorBase()
        {

        }

        public TypeCreatorBase(Type type)
        {
            Type = type;
        }

		protected Func<T, object> GetCreator<T>()
		{
			var ctor = Type.GetConstructorEx(new[] { typeof(T) });
			var parm = Expression.Parameter(typeof(T));
			var expr = Expression.Lambda<Func<T, object>>(
			  Expression.Convert(Expression.New(ctor, 
                Expression.Convert(parm, ctor.GetParameters()[0].ParameterType)), typeof(object)),
			  parm);

			return expr.Compile();
		}

		protected Func<T, object> GetCreator<T>(Type paramType)
		{
			var ctor = Type.GetConstructorEx(new[] { paramType });

			if (ctor == null)
				return null;

			var parm = Expression.Parameter(typeof(T));
			var expr = Expression.Lambda<Func<T, object>>(
			  Expression.Convert(Expression.New(ctor,
                Expression.Convert(parm, ctor.GetParameters()[0].ParameterType)), typeof(object)),
              parm);

			return expr.Compile();
		}

		public static implicit operator Type(TypeCreatorBase typeCreator)
		{
			return typeCreator.Type;
		}

		public bool IsSupported { get { return Type != null; } }
	}
}