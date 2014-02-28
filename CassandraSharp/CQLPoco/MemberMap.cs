// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2014 Pierre Chalamet
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace CassandraSharp.CQLPoco
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Reflection.Emit;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Exceptions;

    internal class MemberMap
    {
        private Func<object, object> _valueGetter;

        private Action<object, object> _valueSetter;

        public MemberMap(ClassMap classMap, MemberInfo memberInfo)
        {
            ClassMap = classMap;
            MemberInfo = memberInfo;

            Type = memberInfo is PropertyInfo
                    ? (memberInfo as PropertyInfo).PropertyType
                    : ((FieldInfo) memberInfo).FieldType;

            ColumnName = GetColumnName(memberInfo);
            ValueSerializer = ValueSerializerProvider.GetSerializer(Type);
        }

        public ClassMap ClassMap { get; private set; }

        public string ColumnName { get; private set; }

        public Type Type { get; private set; }

        public MemberInfo MemberInfo { get; private set; }

        public IValueSerializer ValueSerializer { get; private set; }

        public Action<object, object> SetValue
        {
            get
            {
                if (_valueSetter == null)
                {
                    _valueSetter = GetSetter();
                }

                return _valueSetter;
            }
        }

        public Func<object, object> GetValue
        {
            get
            {
                if (_valueGetter == null)
                {
                    _valueGetter = GetGetter();
                }

                return _valueGetter;
            }
        }

        public object CreateTypeInstance()
        {
            return Activator.CreateInstance(Type);
        }

        public static bool IsIgnored(MemberInfo mi)
        {
            var ignoreAttribute = mi.GetCustomAttributes(typeof(CqlIgnoreAttribute), true).FirstOrDefault() as CqlIgnoreAttribute;
            return ignoreAttribute != null;
        }

        public static string GetColumnName(MemberInfo mi)
        {
            var customColumn = mi.GetCustomAttributes(typeof(CqlColumnAttribute), true).FirstOrDefault() as CqlColumnAttribute;
            if (customColumn != null)
            {
                return customColumn.Name.ToLowerInvariant();
            }

            return mi.Name.ToLowerInvariant();
        }

        private Func<object, object> GetGetter()
        {
            var propertyInfo = MemberInfo as PropertyInfo;
            if (propertyInfo != null)
            {
                var getMethodInfo = propertyInfo.GetGetMethod(true);
                if (getMethodInfo == null)
                {
                    var message = string.Format(
                            "The property '{0} {1}' of class '{2}' has no 'get' accessor.",
                            propertyInfo.PropertyType.FullName, propertyInfo.Name, propertyInfo.DeclaringType.FullName);

                    throw new DataMappingException(message);
                }
            }

            // lambdaExpression = (obj) => (object) ((TClass) obj).Member
            var objParameter = Expression.Parameter(typeof(object), "obj");
            var lambdaExpression = Expression.Lambda<Func<object, object>>(
                    Expression.Convert(
                            Expression.MakeMemberAccess(
                                    Expression.Convert(objParameter, MemberInfo.DeclaringType),
                                    MemberInfo
                                    ),
                            typeof(object)
                            ),
                    objParameter
                    );

            return lambdaExpression.Compile();
        }

        private Action<object, object> GetSetter()
        {
            if (MemberInfo is FieldInfo)
            {
                var fieldInfo = MemberInfo as FieldInfo;
                var sourceType = fieldInfo.DeclaringType;
                var method = new DynamicMethod("Set" + fieldInfo.Name, null, new[] {typeof(object), typeof(object)}, true);
                var gen = method.GetILGenerator();

                gen.Emit(OpCodes.Ldarg_0);
                gen.Emit(OpCodes.Castclass, sourceType);
                gen.Emit(OpCodes.Ldarg_1);
                gen.Emit(OpCodes.Unbox_Any, fieldInfo.FieldType);
                gen.Emit(OpCodes.Stfld, fieldInfo);
                gen.Emit(OpCodes.Ret);

                return (Action<object, object>) method.CreateDelegate(typeof(Action<object, object>));
            }

            var propertyInfo = (PropertyInfo) MemberInfo;
            var setMethodInfo = propertyInfo.GetSetMethod(true);
            if (setMethodInfo == null)
            {
                var message = string.Format(
                        "The property '{0} {1}' of class '{2}' has no 'set' accessor.",
                        propertyInfo.PropertyType.FullName, propertyInfo.Name, propertyInfo.DeclaringType.FullName);
                throw new DataMappingException(message);
            }

            // lambdaExpression = (obj, value) => ((TClass) obj).SetMethod((TMember) value)
            var objParameter = Expression.Parameter(typeof(object), "obj");
            var valueParameter = Expression.Parameter(typeof(object), "value");
            var lambdaExpression = Expression.Lambda<Action<object, object>>(
                    Expression.Call(
                            Expression.Convert(objParameter, MemberInfo.DeclaringType),
                            setMethodInfo,
                            Expression.Convert(valueParameter, Type)
                            ),
                    objParameter,
                    valueParameter
                    );

            return lambdaExpression.Compile();
        }
    }
}