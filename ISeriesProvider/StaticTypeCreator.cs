using System;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
    public class StaticTypeCreator<T> : StaticTypeCreatorBase<T>
    {
        Func<T> _creator;

        public StaticTypeCreator() : base()
        {

        }

        public StaticTypeCreator(Type type) : base(type)
        {

        }

        public T CreateInstance()
        {
            return (_creator ?? (_creator = GetCreator()))();
        }
    }

    public class StaticTypeCreator<T1, T> : StaticTypeCreator<T>
    {
        Func<T1, T> _creator;

        public StaticTypeCreator() : base()
        {

        }

        public StaticTypeCreator(Type type) : base(type)
        {

        }

        public T CreateInstance(T1 value)
        {
            return (_creator ?? (_creator = GetCreator<T1>()))(value);
        }
    }

    public class StaticTypeCreator<T1, T2, T> : StaticTypeCreator<T1, T>
    {
        Func<T2, T> _paramCreator;

        public StaticTypeCreator() : base()
        {

        }

        public StaticTypeCreator(Type type) : base(type)
        {

        }

        public object CreateInstance(T2 value)
        {
            return (_paramCreator ?? (_paramCreator = GetCreator<T2>()))(value);
        }
    }

    public class StaticTypeCreator<T1, T2, T3, T> : StaticTypeCreator<T1, T2, T>
    {
        Func<T3, T> _paramCreator;

        public StaticTypeCreator() : base()
        {

        }

        public StaticTypeCreator(Type type) : base(type)
        {

        }

        public object CreateInstance(T3 value)
        {
            return (_paramCreator ?? (_paramCreator = GetCreator<T3>()))(value);
        }
    }

}
