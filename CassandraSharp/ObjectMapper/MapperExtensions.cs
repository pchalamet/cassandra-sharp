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

namespace CassandraSharp.ObjectMapper
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Apache.Cassandra;
    using CassandraSharp.NameOrValues;

    public static class MapperExtensions
    {
        private class Unit
        {
        }

        private static readonly Dictionary<Type, Func<object, INameOrValue>> _netType2NameOrValueFromValue = 
            new Dictionary<Type, Func<object, INameOrValue>>
                {
                    {typeof(int), x => new IntNameOrValue((int)x)},
                    {typeof(int?), x => null != x ? new IntNameOrValue((int)x) : null},
                    {typeof(long), x => new LongNameOrValue((long)x)},
                    {typeof(long?), x => null != x ? new LongNameOrValue((long)x) : null},
                    {typeof(float), x => new FloatNameOrValue((float)x)},
                    {typeof(float?), x => null != x ? new FloatNameOrValue((float)x) : null},
                    {typeof(double),x => new DoubleNameOrValue((double)x)},
                    {typeof(double?), x => null != x ? new DoubleNameOrValue((double)x) : null},
                    {typeof(string), x => null != x ? new Utf8NameOrValue((string)x) : null },
                    {typeof(DateTime), x => null != x ? new LongNameOrValue(((DateTime)x).Ticks) : null },
                    {typeof(byte[]), x => new ByteArrayNameOrValue((byte[])x)},
                    {typeof(Decimal), x => null },
                    {typeof(Decimal?), x => null},
                    {typeof(Guid), x => new GuidNameOrValue((Guid)x) },
                    {typeof(Guid?), x => null != x ? new GuidNameOrValue((Guid)x) : null },
                };

        private static readonly Dictionary<Type, Func<byte[], INameOrValue>> _netType2NameOrValueFromByteArray =
    new Dictionary<Type, Func<byte[], INameOrValue>>
                {
                    {typeof(int), x => new IntNameOrValue(x)},
                    {typeof(int?), x => new IntNameOrValue(x)},
                    {typeof(long), x => new LongNameOrValue(x)},
                    {typeof(long?), x => new LongNameOrValue(x)},
                    {typeof(float), x => new FloatNameOrValue(x)},
                    {typeof(float?), x => new FloatNameOrValue(x)},
                    {typeof(double),x => new DoubleNameOrValue(x)},
                    {typeof(double?), x => new DoubleNameOrValue(x)},
                    {typeof(string), x => new Utf8NameOrValue(x) },
                    {typeof(DateTime), x => new LongNameOrValue(x) },
                    {typeof(byte[]), x => new ByteArrayNameOrValue(x)},
                    {typeof(Decimal), x => null },
                    {typeof(Decimal?), x => null},
                    {typeof(Guid), x => new GuidNameOrValue(x) },
                    {typeof(Guid?), x => new GuidNameOrValue(x) },
                };

        private static IEnumerable<T> Execute<T>(Cassandra.Client client, CqlPreparedResult preparedStmt, object param)
            where T : new()
        {
            List<byte[]> prms = new List<byte[]>();
            if (null != preparedStmt.Name_types)
            {
                Type paramType = param.GetType();
                foreach (CqlNameType prmDef in preparedStmt.Name_types)
                {
                    string prmName = new Utf8NameOrValue(prmDef.Key).Value;
                    MemberInfo mi = paramType.GetMember(prmName).Single();

                    Type mit;
                    object miv;
                    if (mi.MemberType == MemberTypes.Property)
                    {
                        PropertyInfo pi = (PropertyInfo) mi;
                        mit = pi.PropertyType;
                        miv = pi.GetValue(param, null);
                    }
                    else
                    {
                        FieldInfo fi = (FieldInfo) mi;
                        mit = fi.FieldType;
                        miv = fi.GetValue(param);
                    }

                    Func<object, INameOrValue> converter = _netType2NameOrValueFromValue[mit];
                    byte[] prm = converter(miv).ToByteArray();
                    prms.Add(prm);
                }
            }

            CqlResult result = client.execute_prepared_cql_query(preparedStmt.ItemId, prms);

            Type resultType = typeof(T);
            MemberInfo miKey = resultType.GetMember("Key", BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.GetProperty).Single();
            foreach(CqlRow row in result.Rows)
            {
                T t = new T();
                if( null != miKey)
                {
                    SetMember(t, row.Key, miKey);
                }
                
                foreach (Column col in row.Columns)
                {
                    byte[] colValue = col.Value;
                    string colName = new Utf8NameOrValue(col.Name).Value;
                    MemberInfo mi = resultType.GetMember(colName).Single();

                    SetMember(t, colValue, mi);
                }

                yield return t;
            }
        }

        private static void SetMember<T>(T t, byte[] colValue, MemberInfo mi) where T : new()
        {
            if (null != colValue)
            {
                Type mit;
                if (mi.MemberType == MemberTypes.Property)
                {
                    PropertyInfo pi = (PropertyInfo) mi;
                    mit = pi.PropertyType;
                }
                else
                {
                    FieldInfo fi = (FieldInfo) mi;
                    mit = fi.FieldType;
                }

                Func<byte[], INameOrValue> converter = _netType2NameOrValueFromByteArray[mit];
                object miv = converter(colValue);
                if (mi.MemberType == MemberTypes.Property)
                {
                    PropertyInfo pi = (PropertyInfo) mi;
                    pi.SetValue(t, miv, null);
                }
                else
                {
                    FieldInfo fi = (FieldInfo) mi;
                    fi.SetValue(t, miv);
                }
            }
        }

        private static IEnumerable<T> PrepareAndExecute<T>(Cassandra.Client client, string query, IEnumerable<object> prms)
            where T : new()
        {
            //client.set_cql_version("3.0.0-beta1");

            Utf8NameOrValue novQuery = new Utf8NameOrValue(query);
            CqlPreparedResult preparedStmt = client.prepare_cql_query(novQuery.ToByteArray(), Compression.NONE);

            foreach(object prm in prms)
            {
                foreach(T t in Execute<T>(client, preparedStmt, prm))
                {
                    yield return t;
                }
            }
        }

        public static int Execute(this ICluster cluster, string query, params object[] param)
        {
            int nbResults = cluster.ExecuteCommand(null, client => PrepareAndExecute<Unit>(client.CassandraClient, query, param).Count());
            return nbResults;
        }

        public static IEnumerable<T> Query<T>(this ICluster cluster, string query, object param)
        {


            throw new NotImplementedException();
        }

        //public static IEnumerable<T> Read<T>(this ICluster cluster, object param)
        //{
        //    throw new NotImplementedException();
        //}

        //public static T Write<T>(this ICluster cluster, object param)
        //{
        //    throw new NotImplementedException();
        //}

        //public static void Delete<T>(this ICluster cluster, object param)
        //{
        //    throw new NotImplementedException();
        //}
    }
}