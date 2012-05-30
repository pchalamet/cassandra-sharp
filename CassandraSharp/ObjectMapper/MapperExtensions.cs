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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp.Config;
    using CassandraSharp.MadeSimple;
    using CassandraSharp.ObjectMapper.Cql3;
    using CassandraSharp.ObjectMapper.Dialect;
    using CassandraSharp.Utils;

    public static class MapperExtensions
    {
        public static IEnumerable<T> Select<T>(this ICluster @this, object template) where T : new()
        {
            @this.CheckArgumentNotNull("@this");
            template.CheckArgumentNotNull("template");

            Schema schema = Schema.FromCache(typeof(T));

            IQueryBuilder builder = new QueryBuilder();
            builder.Columns = schema.CqlName2ColumnDefs.Keys.ToArray();
            builder.Table = schema.Table;
            builder.ConsistencyLevel = @this.BehaviorConfig.WriteConsistencyLevel;
            builder.Wheres = template.GetType().GetPublicMembers().Select(x => x.Name + "=?").ToArray();
            string cqlSelect = builder.Build();

            return @this.Execute<T>(schema, cqlSelect, template);
        }

        public static IEnumerable<R> Select<T, R>(this ICluster @this, object template) where R : new()
        {
            @this.CheckArgumentNotNull("@this");
            template.CheckArgumentNotNull("template");

            Schema schema = Schema.FromCache(typeof(T));

            IQueryBuilder builder = new QueryBuilder();
            builder.Columns = schema.CqlName2ColumnDefs.Keys.ToArray();
            builder.Table = schema.Table;
            builder.ConsistencyLevel = @this.BehaviorConfig.WriteConsistencyLevel;
            builder.Wheres = template.GetType().GetPublicMembers().Select(x => x.Name + "=?").ToArray();
            string cqlSelect = builder.Build();

            return @this.Execute<R>(schema, cqlSelect, template);
        }

        public static void Insert<T>(this ICluster @this, object template) where T : new()
        {
            @this.CheckArgumentNotNull("@this");
            template.CheckArgumentNotNull("template");

            Schema schema = Schema.FromCache(typeof(T));

            IInsertBuilder builder = new InsertBuilder();
            builder.Table = schema.Table;
            builder.Columns = template.GetType().GetPublicMembers().Select(x => schema.NetName2ColumnDefs[x.Name].CqlName).ToArray();
            builder.Values = Enumerable.Repeat("?", builder.Columns.Length).ToArray();
            builder.ConsistencyLevel = @this.BehaviorConfig.WriteConsistencyLevel;
            builder.TTL = @this.BehaviorConfig.TTL;
            builder.Timestamp = @this.TimestampService.Generate();
            string cqlInsert = builder.Build();

            @this.ExecuteNonQuery(schema, cqlInsert, template);
        }

        public static void Delete<T>(this ICluster @this, object template) where T : new()
        {
            @this.CheckArgumentNotNull("@this");
            template.CheckArgumentNotNull("template");

            // translate to "delete"
            throw new NotImplementedException();
        }

        public static void Update<T>(this ICluster @this, object template) where T : new()
        {
            @this.CheckArgumentNotNull("@this");
            template.CheckArgumentNotNull("template");

            // translate to "update"
            throw new NotImplementedException();
        }

        public static void CreateKeyspace<T>(this ICluster @this, string strategyClass, Dictionary<string, int> replicationFactor) where T : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(T));

            ICreateKeyspaceBuilder builder = new CreateKeyspaceBuilder();
            builder.Keyspace = schema.Keyspace;
            builder.StrategyClass = strategyClass;
            builder.ReplicationFactor = replicationFactor;
            string createKeyspaceStmt = builder.Build();

            @this.ExecuteCql(createKeyspaceStmt);
        }

        public static void DropKeyspace<T>(this ICluster @this) where T : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(T));

            IDropKeyspaceBuilder builder = new DropKeyspaceBuilder();
            builder.Keyspace = schema.Keyspace;
            string dropKeyspaceStmt = builder.Build();

            @this.ExecuteCql(dropKeyspaceStmt);
        }

        public static void CreateTable<T>(this ICluster @this) where T : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(T));

            ICreateTableBuilder builder = new CreateTableBuilder();
            builder.Table = schema.Table;
            builder.Columns = schema.CqlName2ColumnDefs.Keys.ToArray();
            builder.ColumnTypes = schema.CqlName2ColumnDefs.Values.Select(x => x.CqlTypeName).ToArray();
            builder.Keys = schema.CqlName2ColumnDefs.Values.Where(x => x.IsKeyComponent).OrderBy(x => x.Index).Select(x => x.CqlName).ToArray();
            builder.CompactStorage = schema.CompactStorage;
            string createTableStmt = builder.Build();

            @this.ExecuteCql(createTableStmt);

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

        public static void DropTable<T>(this ICluster @this) where T : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(T));

            IDropTableBuilder builder = new DropTableBuilder();
            builder.Table = schema.Table;
            string dropTableStmt = builder.Build();

            @this.ExecuteCql(dropTableStmt);
        }

        public static void Truncate<T>(this ICluster @this) where T : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(T));

            ITruncateTableBuilder tableBuilder = new TruncateTableBuilder();
            tableBuilder.Table = schema.Table;
            string dropTableStmt = tableBuilder.Build();

            @this.ExecuteCql(dropTableStmt);
        }
    }
}