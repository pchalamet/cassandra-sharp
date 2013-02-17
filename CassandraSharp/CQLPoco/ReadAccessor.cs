// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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
    using System.Reflection;
    using System.Reflection.Emit;

    internal class ReadAccessor<T> : Accessor<T>
    {
        private readonly ReadValue _accessor;

        public ReadAccessor()
        {
            _accessor = Generate();
        }

        public object Get(ref T source, string name)
        {
            return _accessor(ref source, name);
        }

        private ReadValue Generate()
        {
            Type type = typeof(T);
            string methodName = "ReadFromObject" + Guid.NewGuid();
            var dm = new DynamicMethod(methodName,
                                       typeof(object),
                                       new[] {type.MakeByRefType(), typeof(string)},
                                       type.Module,
                                       true);
            var gen = dm.GetILGenerator();

            // [arg_0] : ref T dataSource
            // [arg_1] : memberName

            GenerateAccessor(gen);

            ReadValue readValue = (ReadValue) dm.CreateDelegate(typeof(ReadValue));
            return readValue;
        }

        protected override void GenerateMemberAccess(ILGenerator gen, MemberInfo mi)
        {
            Type type = typeof(T);

            gen.Emit(OpCodes.Ldarg_0); // [ref dataSource]
            if (type.IsClass)
            {
                gen.Emit(OpCodes.Ldind_Ref); // [dataSource]
            }

            PropertyInfo propertyInfo = mi as PropertyInfo;
            Type valueType = null;
            if (null != propertyInfo)
            {
                gen.Emit(OpCodes.Call, propertyInfo.GetGetMethod());
                if (propertyInfo.PropertyType.IsValueType)
                {
                    valueType = propertyInfo.PropertyType;
                }
            }
            else
            {
                FieldInfo fieldInfo = (FieldInfo) mi;
                gen.Emit(OpCodes.Ldfld, fieldInfo);
                if (fieldInfo.FieldType.IsValueType)
                {
                    valueType = fieldInfo.FieldType;
                }
            }

            if (null != valueType)
            {
                gen.Emit(OpCodes.Box, valueType);
            }

            gen.Emit(OpCodes.Ret);
        }

        protected override void GenerateNotFound(ILGenerator gen)
        {
            ConstructorInfo exceptionCtor = typeof(ArgumentException).GetConstructor(BindingFlags.Instance | BindingFlags.Public, null,
                                                                                     new[] {typeof(string), typeof(string)}, null);
            gen.Emit(OpCodes.Ldstr, "Can't find requested member");
            gen.Emit(OpCodes.Ldarg_1);
            gen.Emit(OpCodes.Newobj, exceptionCtor);
            gen.Emit(OpCodes.Throw);
        }

        private delegate object ReadValue(ref T dataSource, string name);
    }
}