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
    using Apache.Cassandra;
    using CassandraSharp.MadeSimple;

    public static class PreparedQueryExtensions
    {
        private static IEnumerable<T> Execute<T>(Cassandra.Client client, CqlPreparedResult preparedStmt, IEnumerable<ColumnDef> allColumns, object param)
            where T : new()
        {
            // feed required columns for the query
            List<byte[]> prms = new List<byte[]>();
            if (null != preparedStmt.Variable_names)
            {
                foreach (string prmName in preparedStmt.Variable_names)
                {
                    ColumnDef columnDef = allColumns.First(x => x.Name == prmName);
                    object value = columnDef.GetValue(param);
                    byte[] prm = columnDef.NetType.Serialize(value);
                    prms.Add(prm);
                }
            }

            // execute the query
            CqlResult result = client.execute_prepared_cql_query(preparedStmt.ItemId, prms);

            // returns results
            if (null != result.Rows)
            {
                Type resultType = typeof(T);
                IEnumerable<ColumnDef> resAllColumns = resultType.FindColumns();

                ColumnDef keyColumn = resAllColumns.FirstOrDefault(x => x.IsKeyComponent && x.Index == 0);
                foreach (CqlRow row in result.Rows)
                {
                    T t = new T();

                    // map key first
                    if (null != keyColumn)
                    {
                        object value = keyColumn.NetType.Deserialize(row.Key);
                        keyColumn.SetValue(t, value);
                    }

                    // map columns
                    foreach (Column col in row.Columns)
                    {
                        string colName = new Utf8NameOrValue(col.Name).Value;
                        ColumnDef colDef = resAllColumns.FirstOrDefault(x => x.Name == colName);
                        if (null != colDef)
                        {
                            object value = colDef.NetType.Deserialize(col.Value);
                            colDef.SetValue(t, value);
                        }
                    }

                    yield return t;
                }
            }
        }

        public static IEnumerable<T> Execute<T>(this ICluster cluster, string query, IEnumerable<object> prms)
            where T : new()
        {
            bool hasError = true;
            IConnection connection = null;
            try
            {
                connection = cluster.AcquireConnection(null);
                Cassandra.Client client = connection.CassandraClient;

                Utf8NameOrValue novQuery = new Utf8NameOrValue(query);
                CqlPreparedResult preparedStmt = connection.CassandraClient.prepare_cql_query(novQuery.ToByteArray(), Compression.NONE);

                foreach (object prm in prms)
                {
                    Type type = prm.GetType();
                    IEnumerable<ColumnDef> allColumns = type.FindColumns();

                    foreach (T unknown in Execute<T>(client, preparedStmt, allColumns, prm))
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
                    cluster.ReleaseConnection(connection, hasError);
                }
            }
        }

        public static IEnumerable<T> Execute<T>(this ICluster cluster, string query, params object[] prms)
            where T : new()
        {
            return cluster.Execute<T>(query, prms.AsEnumerable());
        }

        public static int ExecuteNonQuery(this ICluster cluster, string query, IEnumerable<object> prms)
        {
            int nbResults = Execute<Unit>(cluster, query, prms).Count();
            return nbResults;
        }

        public static int ExecuteNonQuery(this ICluster cluster, string query, params object[] prms)
        {
            return cluster.ExecuteNonQuery(query, prms.AsEnumerable());
        }

        private class Unit
        {
        }
    }
}