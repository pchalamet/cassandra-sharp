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
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Reflection.Emit;

    public class ClassMap
    {
        private static readonly Dictionary<Type, ClassMap> ClassMaps;
        private static readonly Dictionary<Type, ICassandraTypeSerializer> CustomTypeSerializers;

        private readonly List<MemberMap> members = new List<MemberMap>();
        private readonly ILookup<string, MemberMap> memberByName;

        public IEnumerable<MemberMap> Members { get { return members; } }

        static ClassMap()
        {
            ClassMaps = new Dictionary<Type, ClassMap>();
            CustomTypeSerializers = new Dictionary<Type, ICassandraTypeSerializer>();
        }

        protected ClassMap(Type type)
        {
            members = GetMappedMembers(type);
            memberByName = members.ToLookup(x => x.ColumnName);
        }

        public static ClassMap<T> GetClassMap<T>()
        {
            Type type = typeof(T);
            ClassMap map;
            if (!ClassMaps.TryGetValue(type, out map))
            {
                map = new ClassMap<T>();
                ClassMaps[type] = map;
            }

            return (ClassMap<T>)map;
        }

        public MemberMap GetMember(string name)
        {
            return memberByName[name.ToLowerInvariant()].FirstOrDefault();
        }

        private List<MemberMap> GetMappedMembers(Type type)
        {
            const BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            var members = type.GetProperties(bindingFlags).Concat<MemberInfo>(type.GetFields(bindingFlags));

            return members.Where(x => !MemberMap.IsIgnored(x)).Select(x => new MemberMap(this, x)).ToList();
        }
    }

    public class ClassMap<T> : ClassMap
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
            Type type = typeof(T);
            string methodName = "New" + Guid.NewGuid();
            var dm = new DynamicMethod(methodName, type, new Type[0], type.Module, true);
            ILGenerator gen = dm.GetILGenerator();

            if (type.IsClass)
            {
                const BindingFlags ctorFlags = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;
                ConstructorInfo ctor = type.GetConstructor(ctorFlags, null, new Type[0], null);
                if (ctor == null)
                {
                    return () => { throw new MissingMethodException("No parameterless constructor defined for this object."); };
                }

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

            NewInstance newInstance = (NewInstance)dm.CreateDelegate(typeof(NewInstance));
            return newInstance;
        }

        private delegate T NewInstance();
    }
}
