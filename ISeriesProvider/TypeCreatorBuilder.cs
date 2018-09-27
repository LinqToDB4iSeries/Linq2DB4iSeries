using LinqToDB.Extensions;
using System;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
    public static class TypeCreatorBuilder
    {
        #region Return as object

        public static Func<object> BuildTypeCreator(Type type)
        {
            var expr = Expression.Lambda<Func<object>>(
              Expression.Convert(Expression.New(type), typeof(object)));

            return expr.Compile();
        }

        public static Func<T, object> BuildTypeCreator<T>(Type type, Type actualParamType = null)
        {
            var ctor = type.GetConstructorEx(new[] { actualParamType ?? typeof(T) });

            if (ctor == null)
                return null;

            var parm = Expression.Parameter(typeof(T));
            var expr = Expression.Lambda<Func<T, object>>(
              Expression.Convert(Expression.New(ctor,
                Expression.Convert(parm, ctor.GetParameters()[0].ParameterType)), typeof(object)),
              parm);

            return expr.Compile();
        }

        #endregion

        #region Build static Type

        public static Func<T> BuildStaticTypeCreator<T>(Type actualType = null)
        {
            var ctor = (actualType ?? typeof(T)).GetConstructor(new Type[0]);
            Expression body = Expression.New(ctor);
            if (actualType != typeof(T))
                body = Expression.Convert(body, typeof(T));
            var expr = Expression.Lambda<Func<T>>(body);

            return expr.Compile();
        }

        public static Func<TParam, TType> BuildStaticTypeCreator<TParam, TType>(Type actualParamType = null, Type actualType = null)
        {
            var ctor = (actualType ?? typeof(TType)).GetConstructor(new[] { actualParamType ?? typeof(TParam) });
            var parm = Expression.Parameter(typeof(TParam));
            Expression body = Expression.New(ctor,
                actualParamType == typeof(TParam) ?
                (Expression)parm : Expression.Convert(parm, typeof(TParam)));

            if (actualType != typeof(TType))
                body = Expression.Convert(body, typeof(TType));
            var expr = Expression.Lambda<Func<TParam, TType>>(body, parm);

            return expr.Compile();
        }

        #endregion
    }
}