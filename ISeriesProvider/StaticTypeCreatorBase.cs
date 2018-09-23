using System;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	public abstract class StaticTypeCreatorBase<T>
	{
		public Type Type { get; }

        protected StaticTypeCreatorBase()
        {
            Type = typeof(T);
        }

        protected StaticTypeCreatorBase(Type actualType)
        {
            Type = actualType ?? typeof(T);
        }

        protected Func<T> GetCreator()
        {
            var ctor = Type.GetConstructor(new Type[0]);
            Expression body = Expression.New(ctor);
            if (Type != typeof(T))
                body = Expression.Convert(body, typeof(T));
            var expr = Expression.Lambda<Func<T>>(body);

            return expr.Compile();
        }

        protected Func<T1, T> GetCreator<T1>()
		{
			var ctor = Type.GetConstructor(new[] { typeof(T1) });
			var parm = Expression.Parameter(typeof(T1));
            Expression body = Expression.New(ctor, parm);
            if (Type != typeof(T))
                body = Expression.Convert(body, typeof(T));
            var expr = Expression.Lambda<Func<T1, T>>(body, parm);

			return expr.Compile();
		}

		protected Func<T1, T> GetCreator<T1>(Type actualParamType)
		{
			var ctor = Type.GetConstructor(new[] { actualParamType });
            var parm = Expression.Parameter(typeof(T));
            Expression body = Expression.New(ctor,
                actualParamType == typeof(T1) ? 
                (Expression)parm : Expression.Convert(parm, typeof(T1)));
                
            if (Type != typeof(T))
                body = Expression.Convert(body, typeof(T));
            var expr = Expression.Lambda<Func<T1, T>>(body, parm);

			return expr.Compile();
		}

		public static implicit operator Type(StaticTypeCreatorBase<T> typeCreator)
		{
			return typeCreator.Type;
		}
	}
}