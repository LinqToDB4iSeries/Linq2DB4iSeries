using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using LinqToDB.Extensions;
    using System.Linq.Expressions;

    public abstract class DB2iSeriesTypeDescriptorBase
    {
        protected abstract Type GetDB2Type();
        protected DB2iSeriesTypeDescriptorBase(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, bool canBeNull = true)
        {
            DotnetType = dotnetType;
            DatareaderGetMethodName = datareaderGetMethodName;
            DatatypeName = datatypeName;
            DataType = dataType;
            CanBeNull = canBeNull;

            type = new Lazy<Type>(() => GetDB2Type());
            nullValue = new Lazy<object>(() => isNullValueSet ? overridenNullValue : GetNullValue());
        }

        protected DB2iSeriesTypeDescriptorBase(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, object nullValue, bool canBeNull = true)
            : this(dotnetType, dataType, datareaderGetMethodName, datatypeName, canBeNull)
        {
            isNullValueSet = true;
            overridenNullValue = nullValue;
        }

        private readonly Lazy<Type> type;
        private readonly Lazy<object> nullValue;
        private readonly object overridenNullValue;
        private readonly bool isNullValueSet = false;

        public Type Type => type.Value;
        public object NullValue => nullValue.Value;

        public Type DotnetType { get; }
        public string DatareaderGetMethodName { get; }
        public string DatatypeName { get; }
        public DataType DataType { get; }
        public bool CanBeNull { get; }

        private object GetNullValue()
        {
            var field = Type.GetFieldEx("Null");
            if (field == null)
                return null;

            return Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, field), typeof(object))).Compile()();
        }

        public static implicit operator Type(DB2iSeriesTypeDescriptorBase dB2TypeDescriptor) => dB2TypeDescriptor.Type;
    }
}