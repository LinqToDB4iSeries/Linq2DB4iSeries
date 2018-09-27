using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using LinqToDB.Extensions;
    using System.Linq.Expressions;

    public abstract class DB2iSeriesTypeDescriptorBase
    {
        protected abstract Type GetDB2Type();
        

        protected DB2iSeriesTypeDescriptorBase(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, bool canBeNull = true, bool isSupported = true)
        {
            DotnetType = dotnetType;
            DatareaderGetMethodName = datareaderGetMethodName;
            DatatypeName = datatypeName;
            DataType = dataType;
            CanBeNull = canBeNull;
            IsSupported = isSupported;
            ProviderParameterDbType = providerParameterDbType;

            type = IsSupported ? new Lazy<Type>(() => GetDB2Type()) : null;
            nullValue = IsSupported ? new Lazy<object>(() => isNullValueSet ? overridenNullValue : GetNullValue()) : null;
        }

        protected DB2iSeriesTypeDescriptorBase(Type dotnetType, DataType dataType, string datareaderGetMethodName, string datatypeName, int providerParameterDbType, object nullValue, bool canBeNull = true, bool isSupported = true)
            : this(dotnetType, dataType, datareaderGetMethodName, datatypeName, providerParameterDbType, canBeNull)
        {
            isNullValueSet = true;
            overridenNullValue = nullValue;
        }

        private readonly Lazy<Type> type;
        private readonly Lazy<object> nullValue;
        private readonly object overridenNullValue;
        private readonly bool isNullValueSet = false;

        public Type Type => IsSupported ? type.Value : null;
        public object NullValue => IsSupported ? nullValue.Value : null;

        public bool IsSupported { get; }
        public Type DotnetType { get; }
        public string DatareaderGetMethodName { get; }
        public string DatatypeName { get; }
        public DataType DataType { get; }
        public bool CanBeNull { get; }
        public int ProviderParameterDbType { get; }
        private object GetNullValue()
        {
            if (!IsSupported)
                return null;

            var field = Type.GetFieldEx("Null");
            if (field == null)
                return null;

            return Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, field), typeof(object))).Compile()();
        }

        public static implicit operator Type(DB2iSeriesTypeDescriptorBase dB2TypeDescriptor) => dB2TypeDescriptor.Type;
    }
}