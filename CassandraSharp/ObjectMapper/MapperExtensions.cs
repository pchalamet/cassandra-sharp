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
    using CassandraSharp.MadeSimple;
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

        public static IEnumerable<R> Select<T, R>(this ICluster cluster, object template) where R : new()
        {
            Schema schema = new Schema(typeof(T));

            IQueryBuilder builder = new QueryBuilder();
            builder.Columns = schema.CqlName2ColumnDefs.Keys.ToArray();
            builder.Table = schema.Table;
            builder.ConsistencyLevel = cluster.BehaviorConfig.WriteConsistencyLevel;
            builder.Wheres = template.GetType().GetPublicMembers().Select(x => x.Name + "=?").ToArray();
            string cqlSelect = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig { KeySpace = schema.Keyspace };
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                return tmpCluster.Execute<R>(schema, cqlSelect, template);
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

        public static void CreateTable<T>(this ICluster cluster) where T : new()
        {
            Schema schema = new Schema(typeof(T));

            ICreateTableBuilder builder = new CreateTableBuilder();
            builder.Table = schema.Table;
            builder.Columns = schema.CqlName2ColumnDefs.Keys.ToArray();
            builder.ColumnTypes = schema.CqlName2ColumnDefs.Values.Select(x => x.CqlTypeName).ToArray();
            builder.Keys = schema.CqlName2ColumnDefs.Values.Where(x => x.IsKeyComponent).OrderBy(x => x.Index).Select(x => x.CqlName).ToArray();
            builder.CompactStorage = schema.CompactStorage;
            string createTableStmt = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schema.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                tmpCluster.ExecuteCql(createTableStmt);

            //BehaviorConfigBuilder cfgBuilder = new BehaviorConfigBuilder();
            //cfgBuilder.KeySpace = schemaAttribute.Keyspace;
            //using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
            //{
            //    tmpCluster.ExecuteCql(createTableStmt);
            //}

            // create indices then
            //foreach (IndexAttribute ia in indices)
            //{
            //    StringBuilder sbCreateIndex = new StringBuilder();
            //    sbCreateIndex.AppendFormat("create index on '{0}'('{1}')", tableName, ia.Name);

            //    string createIndexStmt = sbCreateIndex.ToString();
            //    tmpCluster.ExecuteCql(createIndexStmt);
            //}
        }

        public static void DropTable<T>(this ICluster cluster) where T : new()
        {
            Schema schema = new Schema(typeof(T));

            IDropTableBuilder builder = new DropTableBuilder();
            builder.Table = schema.Table;
            string dropTableStmt = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schema.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                tmpCluster.ExecuteCql(dropTableStmt);
        }

        public static void Truncate<T>(this ICluster cluster) where T : new()
        {
            Schema schema = new Schema(typeof(T));

            ITruncateTableBuilder tableBuilder = new TruncateTableBuilder();
            tableBuilder.Table = schema.Table;
            string dropTableStmt = tableBuilder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schema.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                tmpCluster.ExecuteCql(dropTableStmt);
        }
    }
}