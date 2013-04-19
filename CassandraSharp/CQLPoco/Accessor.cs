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
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Reflection.Emit;

    internal abstract class Accessor<T>
    {
        protected void GenerateAccessor(ILGenerator gen)
        {
            Type type = typeof(T);

            // [loc 0] hashCode of method name
            gen.DeclareLocal(typeof(int));

            MethodInfo strGetHashCode = typeof(string).GetMethod("GetHashCode",
                                                                 BindingFlags.Instance | BindingFlags.Public,
                                                                 null,
                                                                 new Type[0],
                                                                 null);

            MethodInfo strToLower = typeof(string).GetMethod("ToLower",
                                                             BindingFlags.Instance | BindingFlags.Public,
                                                             null,
                                                             new[] {typeof(CultureInfo)},
                                                             null);

            MethodInfo getInvariantCulture = typeof(CultureInfo).GetMethod("get_InvariantCulture",
                                                                           BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic,
                                                                           null,
                                                                           new Type[0],
                                                                           null);

            const BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            SortedDictionary<int, List<MemberInfo>> hash2Mis = new SortedDictionary<int, List<MemberInfo>>();
            IEnumerable<MemberInfo> memberInfos = type.GetProperties(bindingFlags).Concat<MemberInfo>(type.GetFields(bindingFlags));

            foreach (MemberInfo memberInfo in memberInfos)
            {
                string name = memberInfo.Name.ToLower(CultureInfo.InvariantCulture);
                int hash = name.GetHashCode();

                List<MemberInfo> mis;
                if (!hash2Mis.TryGetValue(hash, out mis))
                {
                    mis = new List<MemberInfo>();
                    hash2Mis.Add(hash, mis);
                }

                mis.Add(memberInfo);
            }

            HashWithMemberInfos[] hashWithMemberInfos = new HashWithMemberInfos[hash2Mis.Count];
            int offset = 0;
            foreach (KeyValuePair<int, List<MemberInfo>> h2Mi in hash2Mis)
            {
                hashWithMemberInfos[offset].HashCode = h2Mi.Key;
                hashWithMemberInfos[offset].MemberInfos = h2Mi.Value.ToArray();
                ++offset;
            }

            // get hashcode and store it in [loc 0]
            gen.Emit(OpCodes.Ldarg_1);
            gen.Emit(OpCodes.Call, getInvariantCulture);
            gen.Emit(OpCodes.Call, strToLower);
            gen.Emit(OpCodes.Call, strGetHashCode);
            gen.Emit(OpCodes.Stloc_0);

            Label notFound = gen.DefineLabel();

            GenerateBinarySearchRec(gen, notFound, hashWithMemberInfos, 0, hashWithMemberInfos.Length - 1);

            // member not found
            gen.MarkLabel(notFound);
            GenerateNotFound(gen);
        }

        private void GenerateBinarySearchRec(ILGenerator gen, Label notFound, HashWithMemberInfos[] hashWithMemberInfos, int min, int max)
        {
            if (max < min)
            {
                gen.Emit(OpCodes.Br, notFound);
                return;
            }

            int mid = (max + min) / 2;

            Label seachLeftLabel = gen.DefineLabel();
            Label seachRightLabel = gen.DefineLabel();

            int midHash = hashWithMemberInfos[mid].HashCode;
            gen.Emit(OpCodes.Ldc_I4, midHash); // [midHash]
            gen.Emit(OpCodes.Ldloc_0); // [midHash] [hash]
            gen.Emit(OpCodes.Clt); // [midHash < hash ?]
            gen.Emit(OpCodes.Brtrue, seachRightLabel);

            gen.Emit(OpCodes.Ldc_I4, midHash); // [midHash]
            gen.Emit(OpCodes.Ldloc_0); // [midHash] [hash]
            gen.Emit(OpCodes.Cgt); // [midHash > hash ?]
            gen.Emit(OpCodes.Brtrue, seachLeftLabel);

            // midHash == hash
            GenerateCheckAndGenMemberAccess(gen, hashWithMemberInfos[mid]);
            gen.Emit(OpCodes.Br, notFound);

            // midHash < hash
            gen.MarkLabel(seachLeftLabel);
            GenerateBinarySearchRec(gen, notFound, hashWithMemberInfos, min, mid - 1);

            // midHash > hash
            gen.MarkLabel(seachRightLabel);
            GenerateBinarySearchRec(gen, notFound, hashWithMemberInfos, mid + 1, max);
        }

        private void GenerateCheckAndGenMemberAccess(ILGenerator gen, HashWithMemberInfos hashWithMemberInfo)
        {
            MethodInfo strCompare = typeof(string).GetMethod("Compare",
                                                             BindingFlags.Static | BindingFlags.Public,
                                                             null,
                                                             new[]
                                                                 {
                                                                         typeof(String),
                                                                         typeof(String),
                                                                         typeof(StringComparison)
                                                                 },
                                                             null);

            Label nextLabel = gen.DefineLabel();
            foreach (MemberInfo mi in hashWithMemberInfo.MemberInfos)
            {
                gen.Emit(OpCodes.Ldarg_1);
                gen.Emit(OpCodes.Ldstr, mi.Name);
                gen.Emit(OpCodes.Ldc_I4_3);
                gen.Emit(OpCodes.Call, strCompare);
                gen.Emit(OpCodes.Brtrue_S, nextLabel);

                GenerateMemberAccess(gen, mi);

                gen.MarkLabel(nextLabel);
                nextLabel = gen.DefineLabel();
            }
        }

        protected abstract void GenerateMemberAccess(ILGenerator gen, MemberInfo mi);

        protected abstract void GenerateNotFound(ILGenerator gen);

        internal struct HashWithMemberInfos
        {
            public int HashCode;

            public MemberInfo[] MemberInfos;
        }
    }
}