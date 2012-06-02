// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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

namespace CassandraSharp.ObjectMapper
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Apache.Cassandra;
    using CassandraSharp.MadeSimple;

    internal static class PreparedQueryExtension
    {
        private static Dictionary<string, object> ToDictionary(this object @this, Schema schema)
        {
            Dictionary<string, object> dic = new Dictionary<string, object>();
            foreach (MemberInfo mi in @this.GetType().GetPublicMembers())
            {
                string name = mi.Name;
                object value = @this.GetDuckValue(name);
                if (null != value)
                {
                    string cqlName = schema.NetName2ColumnDefs[name].CqlName;
                    dic.Add(cqlName, value);
                }
            }

            return dic;
        }

        private static IEnumerable<TR> Execute<TR>(Cassandra.Client client, Schema schema, CqlPreparedResult preparedStmt, Dictionary<string, object> param)
            where TR : new()
        {
            // feed required columns for the query
            List<byte[]> prms = new List<byte[]>();
            if (null != preparedStmt.Variable_names)
            {
                foreach (string prmName in preparedStmt.Variable_names)
                {
                    // find the ColumnDef in the object model from C* response
                    object value = param[prmName];
                    byte[] prm = null != value
                                     ? value.GetType().Serialize(value)
                                     : null;
                    prms.Add(prm);
                }
            }

            // execute the query
            CqlResult result = client.execute_prepared_cql_query(preparedStmt.ItemId, prms);

            // returns results
            if (null != result.Rows)
            {
                foreach (CqlRow row in result.Rows)
                {
                    TR t = new TR();

                    // map columns
                    foreach (Column col in row.Columns)
                    {
                        string colName = new Utf8NameOrValue(col.Name).Value;
                        ColumnDef colDef;
                        if (schema.CqlName2ColumnDefs.TryGetValue(colName, out colDef))
                        {
                            if (null != col.Value)
                            {
                                object value = colDef.NetType.Deserialize(col.Value);
                                if (null != value)
                                {
                                    t.SetDuckValue(colDef.NetName, value);
                                }
                            }
                        }
                    }

                    yield return t;
                }
            }
        }

        private static IEnumerable<TR> Execute<TR>(this ICluster @this, Schema schema, string query, IEnumerable<Dictionary<string, object>> prms)
            where TR : new()
        {
            using (IConnection connection = @this.AcquireConnection(null))
            {
                Cassandra.Client client = connection.CassandraClient;

                Utf8NameOrValue novQuery = new Utf8NameOrValue(query);
                CqlPreparedResult preparedStmt = connection.CassandraClient.prepare_cql_query(novQuery.ConvertToByteArray(), Compression.NONE);

                foreach (Dictionary<string, object> prm in prms)
                {
                    foreach (TR unknown in Execute<TR>(client, schema, preparedStmt, prm))
                    {
                        yield return unknown;
                    }
                }

                connection.KeepAlive();
            }
        }

        // =========================================================
        // QUERY
        // =========================================================
        internal static IEnumerable<TR> Execute<TR>(this ICluster @this, Schema schema, string query, params Dictionary<string, object>[] prms) where TR : new()
        {
            return @this.Execute<TR>(schema, query, prms.AsEnumerable());
        }

        public static IEnumerable<TR> Execute<TR>(this ICluster @this, Schema schema, string query, IEnumerable<object> prms) where TR : new()
        {
            IEnumerable<Dictionary<string, object>> dicPrms = prms.Select(x => x.ToDictionary(schema));
            return @this.Execute<TR>(schema, query, dicPrms);
        }

        public static IEnumerable<TR> Execute<TR>(this ICluster @this, Schema schema, string query, params object[] prms) where TR : new()
        {
            return @this.Execute<TR>(schema, query, prms.AsEnumerable());
        }

        // =========================================================
        // NON QUERY
        // =========================================================
        internal static int ExecuteNonQuery(this ICluster @this, Schema schema, string query, params Dictionary<string, object>[] prms)
        {
            int nbResults = @this.Execute<Unit>(schema, query, prms.AsEnumerable()).Count();
            return nbResults;
        }

        public static int ExecuteNonQuery(this ICluster @this, Schema schema, string query, IEnumerable<object> prms)
        {
            IEnumerable<Dictionary<string, object>> dicPrms = prms.Select(x => x.ToDictionary(schema));
            int nbResults = @this.Execute<Unit>(schema, query, dicPrms).Count();
            return nbResults;
        }

        public static int ExecuteNonQuery(this ICluster @this, Schema schema, string query, params object[] prms)
        {
            return @this.ExecuteNonQuery(schema, query, prms.AsEnumerable());
        }

        private class Unit
        {
        }
    }
}