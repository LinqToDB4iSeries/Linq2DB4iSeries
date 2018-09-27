using LinqToDB.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqToDB.DataProvider.DB2iSeries
{
    internal static class MemberAccessor
    {
        private struct MemberAccessorKey
        {
            public Type Type { get; }
            public Type AccessorType { get; }
            public MemberInfo MemberInfo { get; }
            public MemberAccessorKey(Type type, Type accessorType, MemberInfo memberInfo)
            {
                Type = type;
                MemberInfo = memberInfo;
                AccessorType = accessorType;
            }
        }

        static readonly Dictionary<MemberAccessorKey, Delegate> getterCache = new Dictionary<MemberAccessorKey, Delegate>();
        static readonly Dictionary<MemberAccessorKey, Delegate> setterCache = new Dictionary<MemberAccessorKey, Delegate>();

        public static Delegate BuildGetAccessor(Type type, Type accessorType, MemberInfo memberInfo)
        {
            var parameter = Expression.Parameter(type);

            var castedParamter =
                type != memberInfo.DeclaringType ?
                Expression.Convert(parameter, memberInfo.DeclaringType) : (Expression)parameter;

            var memberType = GetMemberType(memberInfo);
            var delegateType = Expression.GetDelegateType(new[] { typeof(object), accessorType });

            Expression propertyExpression = Expression.PropertyOrField(castedParamter, memberInfo.Name);
            if (accessorType != memberType)
                propertyExpression = Expression.Convert(propertyExpression, accessorType);

            return Expression.Lambda(delegateType, propertyExpression, parameter).Compile();
        }

        public static Delegate BuildSetAccessor(Type type, Type accessorType, MemberInfo memberInfo)
        {
            var parameter = Expression.Parameter(type);
            var valueParameter = Expression.Parameter(accessorType);

            var castedParamter =
                type != memberInfo.DeclaringType ?
                Expression.Convert(parameter, memberInfo.DeclaringType) : (Expression)parameter;

            var memberType = GetMemberType(memberInfo);
            var delegateType = Expression.GetDelegateType(new[] { typeof(object), accessorType, typeof(void) });

            var castedValueParameter =
                accessorType != memberType ?
                Expression.Convert(valueParameter, memberType) : (Expression)valueParameter;

            var propertyExpression = Expression.PropertyOrField(castedParamter, memberInfo.Name);

            var assignmentExpression = Expression.Assign(propertyExpression, castedValueParameter);

            return Expression.Lambda(delegateType, assignmentExpression, parameter, valueParameter).Compile();
        }

        private static Type GetMemberType(MemberInfo memberInfo)
        {
            if (memberInfo is PropertyInfo propertyInfo)
                return propertyInfo.PropertyType;
            else if (memberInfo is FieldInfo fieldInfo)
                return fieldInfo.FieldType;
            return null;
        }

        private static bool CanGet(MemberInfo memberInfo)
        {
            if (memberInfo is PropertyInfo propertyInfo)
                return propertyInfo.CanRead;
            else if (memberInfo is FieldInfo fieldInfo)
                return true;
            else
                return false;
        }

        private static bool CanSet(MemberInfo memberInfo)
        {
            if (memberInfo is PropertyInfo propertyInfo)
                return propertyInfo.CanWrite;
            else if (memberInfo is FieldInfo fieldInfo)
                return true;
            else
                return false;
        }

        private static MemberInfo GetMemberInfo(Type type, string memberName)
        {
            if (TryGetMemberInfo(type, memberName, out var memberInfo))
                return memberInfo;

            throw new InvalidOperationException($"Property or field {memberName} not found in type {type.FullName}");
        }

        private static bool TryGetMemberInfo(Type type, string memberName, out MemberInfo memberInfo)
        {
            var propertyInfo = type.GetPropertyEx(memberName);
            var fieldInfo = (propertyInfo is null) ?
                 type.GetField(memberName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) : null;

            memberInfo = (MemberInfo)propertyInfo ?? (MemberInfo)fieldInfo;

            return !(memberInfo is null);
        }

        public static TMember GetValue<TMember>(object instance, string memberName)
        {
            if (instance is null) throw new ArgumentNullException(nameof(instance));

            var type = instance.GetType();
            var memberInfo = GetMemberInfo(type, memberName);
            var key = new MemberAccessorKey(type, typeof(TMember), memberInfo);
            if (!getterCache.TryGetValue(key, out var accessor))
            {
                accessor = BuildGetAccessor(typeof(object), typeof(TMember), memberInfo);
                getterCache[key] = accessor;
            }

            return ((Func<object, TMember>)accessor)(instance);
        }

        public static bool TryGetValue<TMember>(object instance, string memberName, out TMember value)
        {
            value = default;

            if (instance is null) return false;

            try
            {
                value = GetValue<TMember>(instance, memberName);
            }
            catch { return false; }

            return true;
        }

        public static void SetValue<TMember>(object instance, string memberName, TMember value)
        {
            if (instance is null) throw new ArgumentNullException(nameof(instance));

            var type = instance.GetType();
            var memberInfo = GetMemberInfo(type, memberName);
            var key = new MemberAccessorKey(type, typeof(TMember), memberInfo);
            if (!setterCache.TryGetValue(key, out var accessor))
            {
                accessor = BuildSetAccessor(typeof(object),typeof(TMember), memberInfo);
                setterCache[key] = accessor;
            }

            ((Action<object, TMember>)accessor)(instance, value);
        }

        public static bool TrySetValue<TMember>(object instance, string memberName, out TMember value)
        {
            value = default;

            if (instance is null) return false;

            try
            {
                SetValue<TMember>(instance, memberName, value);
            }
            catch { return false; }

            return true;
        }
    }
}
