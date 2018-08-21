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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using CassandraSharp.CQLPoco;

namespace CassandraSharp.Core.CQLPoco
{
    internal class ClassMap
    {
        private static readonly Dictionary<Type, ClassMap> _classMaps;

        private readonly ILookup<string, MemberMap> _memberByName;

        static ClassMap()
        {
            _classMaps = new Dictionary<Type, ClassMap>();
        }

        protected ClassMap(Type type)
        {
            var members = GetMappedMembers(type);
            _memberByName = members.ToLookup(x => x.ColumnName);
        }

        public static ClassMap<T> GetClassMap<T>()
        {
            var type = typeof(T);
            ClassMap map;
            if (!_classMaps.TryGetValue(type, out map))
            {
                map = new ClassMap<T>();
                _classMaps[type] = map;
            }

            return (ClassMap<T>)map;
        }

        public MemberMap GetMember(string name)
        {
            return _memberByName[name.ToLowerInvariant()].FirstOrDefault();
        }

        private IEnumerable<MemberMap> GetMappedMembers(Type type)
        {
            const BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            var members = type.GetProperties(bindingFlags).Concat<MemberInfo>(type.GetFields(bindingFlags));

            return members.Where(x => !MemberMap.IsIgnored(x)).Select(x => new MemberMap(this, x));
        }
    }

    internal class ClassMap<T> : ClassMap
    {
        private readonly NewInstance _newInstance;

        protected internal ClassMap()
            : base(typeof(T))
        {
            _newInstance = GenerateNew();
        }

        public T CreateNewInstance()
        {
            return _newInstance();
        }

        private static NewInstance GenerateNew()
        {
            var type = typeof(T);
            var methodName = "New" + Guid.NewGuid();
            var dm = new DynamicMethod(methodName, type, new Type[0], type.Module, true);
            var gen = dm.GetILGenerator();

            if (type.IsClass)
            {
                const BindingFlags ctorFlags = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;
                var ctor = type.GetConstructor(ctorFlags, null, new Type[0], null);
                if (ctor == null) return () => { throw new MissingMethodException("No parameterless constructor defined for this object."); };

                gen.Emit(OpCodes.Newobj, ctor);
            }
            else
            {
                gen.DeclareLocal(type);

                gen.Emit(OpCodes.Ldloca_S, 0);
                gen.Emit(OpCodes.Initobj, type);
                gen.Emit(OpCodes.Ldloc_0);
            }

            gen.Emit(OpCodes.Ret);

            var newInstance = (NewInstance)dm.CreateDelegate(typeof(NewInstance));
            return newInstance;
        }

        private delegate T NewInstance();
    }
}