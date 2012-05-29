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
    using CassandraSharp.Config;
    using CassandraSharp.ObjectMapper.Cql3;
    using CassandraSharp.ObjectMapper.Dialect;

    public static class MapperExtensions
    {
        public static IEnumerable<T> Select<T>(this ICluster cluster, object template) where T : new()
        {
            Schema schema = new Schema(typeof(T));

            IQueryBuilder builder = new QueryBuilder();
            builder.Columns = schema.CqlName2ColumnDefs.Keys.ToArray();
            builder.Table = schema.Table;
            builder.ConsistencyLevel = cluster.BehaviorConfig.WriteConsistencyLevel;
            builder.Wheres = template.GetType().GetPublicMembers().Select(x => x.Name + "=?").ToArray();
            string cqlSelect = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schema.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                return tmpCluster.Execute<T>(schema, cqlSelect, template);
        }

        public static void Insert<T>(this ICluster cluster, object param) where T : new()
        {
            Schema schema = new Schema(typeof(T));

            IInsertBuilder builder = new InsertBuilder();
            builder.Table = schema.Table;
            builder.Columns = param.GetType().GetPublicMembers().Select(x => schema.NetName2ColumnDefs[x.Name].CqlName).ToArray();
            builder.Values = Enumerable.Repeat("?", builder.Columns.Length).ToArray();
            builder.ConsistencyLevel = cluster.BehaviorConfig.WriteConsistencyLevel;
            builder.TTL = cluster.BehaviorConfig.TTL;
            builder.Timestamp = cluster.TimestampService.Generate();
            string cqlInsert = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schema.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                tmpCluster.ExecuteNonQuery(schema, cqlInsert, param);
        }

        public static void Delete<T>(this ICluster cluster, object template) where T : new()
        {
            // translate to "delete"
            throw new NotImplementedException();
        }

        public static void Update<T>(this ICluster cluster, object param) where T : new()
        {
            // translate to "update"
            throw new NotImplementedException();
        }
    }
}