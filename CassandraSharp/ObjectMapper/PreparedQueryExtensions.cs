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
    using Apache.Cassandra;
    using CassandraSharp.MadeSimple;

    internal static class PreparedQueryExtension
    {
        private static IEnumerable<T> Execute<T>(Cassandra.Client client, Schema schema, CqlPreparedResult preparedStmt, object param)
            where T : new()
        {
            // feed required columns for the query
            List<byte[]> prms = new List<byte[]>();
            if (null != preparedStmt.Variable_names)
            {
                foreach (string prmName in preparedStmt.Variable_names)
                {
                    // find the ColumnDef in the object model from C* response
                    ColumnDef columnDef = schema.CqlName2ColumnDefs[prmName];
                    object value = param.GetDuckValue(columnDef.NetName);
                    byte[] prm = value.GetType().Serialize(value);
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
                    T t = new T();

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

        public static IEnumerable<T> Execute<T>(this ICluster @this, Schema schema, string query, IEnumerable<object> prms) where T : new()
        {
            bool hasError = true;
            IConnection connection = null;
            try
            {
                connection = @this.AcquireConnection(null);
                Cassandra.Client client = connection.CassandraClient;

                Utf8NameOrValue novQuery = new Utf8NameOrValue(query);
                CqlPreparedResult preparedStmt = connection.CassandraClient.prepare_cql_query(novQuery.ToByteArray(), Compression.NONE);

                foreach (object prm in prms)
                {
                    foreach (T unknown in Execute<T>(client, schema, preparedStmt, prm))
                    {
                        yield return unknown;
                    }
                }
                hasError = false;
            }
            finally
            {
                if (null != connection)
                {
                    @this.ReleaseConnection(connection, hasError);
                }
            }
        }

        public static IEnumerable<T> Execute<T>(this ICluster @this, Schema schema, string query, params object[] prms)
            where T : new()
        {
            return @this.Execute<T>(schema, query, prms.AsEnumerable());
        }

        public static int ExecuteNonQuery(this ICluster @this, Schema schema, string query, IEnumerable<object> prms)
        {
            int nbResults = @this.Execute<Unit>(schema, query, prms).Count();
            return nbResults;
        }

        public static int ExecuteNonQuery(this ICluster @this, Schema schema, string query, params object[] prms)
        {
            return @this.ExecuteNonQuery(schema, query, prms.AsEnumerable());
        }

        internal class Unit
        {
        }
    }
}