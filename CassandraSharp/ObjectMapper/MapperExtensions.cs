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
    using System.Text;
    using Apache.Cassandra;
    using CassandraSharp.MadeSimple;

    public static class MapperExtensions
    {
        private class Unit
        {
        }

        private static IEnumerable<T> Execute<T>(Cassandra.Client client, CqlPreparedResult preparedStmt, object param)
            where T : new()
        {
            List<byte[]> prms = new List<byte[]>();
            if (null != preparedStmt.Variable_names)
            {
                Type paramType = param.GetType();
                foreach (string prmName in preparedStmt.Variable_names)
                {
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

                    byte[] prm = mit.SerializeValue(miv);
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

                object miv = mit.DeserializeValue(colValue);
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

            return prms.SelectMany(prm => Execute<T>(client, preparedStmt, prm));
        }

        public static int Execute(this ICluster cluster, string query, params object[] param)
        {
            int nbResults = cluster.ExecuteCommand(null, client => PrepareAndExecute<Unit>(client.CassandraClient, query, param).Count());
            return nbResults;
        }

        public static void Write<T>(this ICluster cluster, T t)
        {
            Type type = typeof(T);

            SchemaAttribute schemaAttribute = type.FindSchemaAttribute();
            string tableName = schemaAttribute.Name ?? type.Name;
            
            IEnumerable<ColumnDef> allColumns = type.FindColumns();

            StringBuilder sbInsert = new StringBuilder("insert into ");
            StringBuilder sbJokers = new StringBuilder("(");
            sbInsert.AppendFormat("{0} (", tableName);
            string sep = "";
            foreach(ColumnDef columnDef in allColumns)
            {
                object value = columnDef.GetValue(t);
                if( null != value)
                {
                    sbInsert.Append(sep).Append(columnDef.Name);
                    sbJokers.Append(sep).Append("?");
                    sep = ", ";
                }
            }
            sbInsert.Append(" ) values ").Append(sbJokers).Append(" )");

            string cqlInsert = sbInsert.ToString();
            cluster.Execute(cqlInsert, t);
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