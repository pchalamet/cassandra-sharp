// cassandra-sharp - high performance .NET driver for Apache Cassandra
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

    internal class WriteAccessor<T> : Accessor<T>
    {
        private readonly WriteValue _writeValue;

        public WriteAccessor()
        {
            _writeValue = Generate();
        }

        public bool Set(ref T target, string name, object value)
        {
            return _writeValue(ref target, name, value);
        }

        private WriteValue Generate()
        {
            Type type = typeof(T);
            string methodName = "WriteToObject" + Guid.NewGuid();
            var dm = new DynamicMethod(methodName,
                                       typeof(bool),
                                       new[] {type.MakeByRefType(), typeof(string), typeof(object)},
                                       type.Module,
                                       true);
            ILGenerator gen = dm.GetILGenerator();

            // [arg_0] : ref T instance
            // [arg_1] : memberName
            // [arg_2] : value

            GenerateAccessor(gen);

            WriteValue writeValue = (WriteValue) dm.CreateDelegate(typeof(WriteValue));
            return writeValue;
        }

        protected override void GenerateMemberAccess(ILGenerator gen, MemberInfo mi)
        {
            Type type = typeof(T);

            gen.Emit(OpCodes.Ldarg_0); // [ref dataSource]
            if (type.IsClass)
            {
                gen.Emit(OpCodes.Ldind_Ref);
            }
            gen.Emit(OpCodes.Ldarg_2); // [dataSource] [value]

            PropertyInfo propertyInfo = mi as PropertyInfo;
            if (null != propertyInfo)
            {
                gen.Emit(propertyInfo.PropertyType.IsClass
                                 ? OpCodes.Castclass
                                 : OpCodes.Unbox_Any, propertyInfo.PropertyType);

                gen.Emit(OpCodes.Call, propertyInfo.GetSetMethod());
            }
            else
            {
                FieldInfo fieldInfo = (FieldInfo) mi;
                gen.Emit(fieldInfo.FieldType.IsClass
                                 ? OpCodes.Castclass
                                 : OpCodes.Unbox_Any, fieldInfo.FieldType);

                gen.Emit(OpCodes.Stfld, fieldInfo);
            }

            gen.Emit(OpCodes.Ldc_I4_1);
            gen.Emit(OpCodes.Ret);
        }

        protected override void GenerateNotFound(ILGenerator gen)
        {
            gen.Emit(OpCodes.Ldc_I4_0);
            gen.Emit(OpCodes.Ret);
        }

        private delegate bool WriteValue(ref T dataSource, string name, object value);
    }
}