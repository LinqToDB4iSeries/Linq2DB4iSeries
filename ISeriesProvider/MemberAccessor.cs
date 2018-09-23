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
            public MemberInfo MemberInfo { get; }
            public MemberAccessorKey(Type type, MemberInfo memberInfo)
            {
                Type = type;
                MemberInfo = memberInfo;
            }
        }

        static readonly Dictionary<MemberAccessorKey, Delegate> cache = new Dictionary<MemberAccessorKey, Delegate>();

        public static Delegate BuildAccessor(Type type, MemberInfo memberInfo)
        {
            var parameter = Expression.Parameter(type);

            var castedParamter =
                type != memberInfo.DeclaringType ?
                Expression.Convert(parameter, memberInfo.DeclaringType) : (Expression)parameter;

            var delegateType = Expression.GetDelegateType(new[] { typeof(object), GetMemberReturnType(memberInfo) });
            return Expression.Lambda(delegateType, Expression.PropertyOrField(castedParamter, memberInfo.Name), parameter).Compile();
        }

        private static Type GetMemberReturnType(MemberInfo memberInfo)
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
            var key = new MemberAccessorKey(type, memberInfo);
            if (!cache.TryGetValue(key, out var accessor))
            {
                accessor = BuildAccessor(typeof(object), memberInfo);
                cache[key] = accessor;
            }

            return ((Func<object, TMember>)accessor)(instance);
        }

        public static bool TryGetValue<TMember>(object instance, string memberName, out TMember value)
        {
            value = default;

            if (instance is null) return false;

            var type = instance.GetType();
            var memberInfo = GetMemberInfo(type, memberName);
            if (!(CanGet(memberInfo)))
                return false;

            var key = new MemberAccessorKey(type, memberInfo);
            if (!cache.TryGetValue(key, out var accessor))
            {
                accessor = BuildAccessor(typeof(object), memberInfo);
                cache[key] = accessor;
            }

            try
            {
                value = ((Func<object, TMember>)accessor)(instance);
            }
            catch { return false; }

            return true;
        }
    }
}
