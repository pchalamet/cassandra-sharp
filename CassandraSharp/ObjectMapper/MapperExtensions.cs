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
    using CassandraSharp.MadeSimple;
    using CassandraSharp.ObjectMapper.Cql3;
    using CassandraSharp.ObjectMapper.Dialect;
    using CassandraSharp.Utils;

    public static class MapperExtensions
    {
        public static IEnumerable<TS> Select<TS>(this ICluster @this, object where) where TS : new()
        {
            return @this.Select<TS, TS>(where);
        }

        public static IEnumerable<TR> Select<TS, TR>(this ICluster @this, object where) where TR : new()
        {
            @this.CheckArgumentNotNull("@this");
            where.CheckArgumentNotNull("where");

            Schema schema = Schema.FromCache(typeof(TS));

            // where is composed with non null fields
            Dictionary<string, object> dic = new Dictionary<string, object>();
            foreach (MemberInfo mi in where.GetType().GetPublicMembers())
            {
                string name = mi.Name;
                object value = where.GetDuckValue(name);
                if (null != value)
                {
                    string cqlName = schema.NetName2ColumnDefs[name].CqlName;
                    dic.Add(cqlName, value);
                }
            }

            IQueryBuilder builder = new QueryBuilder();
            builder.Table = schema.Table;
            builder.Columns = schema.CqlName2ColumnDefs.Keys.ToArray();
            builder.Wheres = dic.Keys.Select(x => x + "=?").ToArray();
            builder.ConsistencyLevel = @this.BehaviorConfig.WriteConsistencyLevel;

            string cql = builder.Build();
            return @this.Execute<TR>(schema, cql, dic);
        }

        public static void Insert<TS>(this ICluster @this, object template) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");
            template.CheckArgumentNotNull("template");

            Schema schema = Schema.FromCache(typeof(TS));

            // insert only non null values
            Dictionary<string, object> dic = new Dictionary<string, object>();
            foreach (MemberInfo mi in template.GetType().GetPublicMembers())
            {
                string name = mi.Name;
                object value = template.GetDuckValue(name);
                if (null != value)
                {
                    string cqlName = schema.NetName2ColumnDefs[name].CqlName;
                    dic.Add(cqlName, value);
                }
            }

            IInsertBuilder builder = new InsertBuilder();
            builder.Table = schema.Table;
            builder.Columns = dic.Keys.ToArray();
            builder.Values = Enumerable.Repeat("?", builder.Columns.Length).ToArray();
            builder.ConsistencyLevel = @this.BehaviorConfig.WriteConsistencyLevel;
            builder.TTL = @this.BehaviorConfig.TTL;
            builder.Timestamp = @this.TimestampService.Generate();

            string cql = builder.Build();
            @this.ExecuteNonQuery(schema, cql, dic);
        }

        public static void Delete<TS>(this ICluster @this, object template, object where) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(TS));

            IDeleteBuilder builder = new DeleteBuilder();
            builder.Table = schema.Table;

            Dictionary<string, object> dic = new Dictionary<string, object>();

            // if template is provided then just delete available fields
            if (null != template)
            {
                foreach (MemberInfo mi in template.GetType().GetPublicMembers())
                {
                    string name = mi.Name;
                    object value = template.GetDuckValue(name);
                    if (null != value)
                    {
                        string cqlName = schema.NetName2ColumnDefs[name].CqlName;
                        dic.Add(cqlName, value);
                    }
                }

                builder.Columns = dic.Keys.ToArray();
            }
            else
            {
                // no template then delete the entire row
                builder.Columns = new string[0];
            }

            // if where is provided then use all non null fields as filter
            if (null != where)
            {
                List<string> wheres = new List<string>();
                foreach (MemberInfo mi in where.GetType().GetPublicMembers())
                {
                    string name = mi.Name;
                    object value = where.GetDuckValue(name);
                    if (null != value)
                    {
                        string cqlName = schema.NetName2ColumnDefs[name].CqlName;
                        dic.Add(cqlName, value);
                        wheres.Add(cqlName + "=?");
                    }
                }

                builder.Wheres = wheres.ToArray();
            }
            builder.ConsistencyLevel = @this.BehaviorConfig.WriteConsistencyLevel;

            string cql = builder.Build();
            @this.ExecuteNonQuery(schema, cql, dic);
        }

        public static void Update<TS>(this ICluster @this, object template, object where) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");
            template.CheckArgumentNotNull("template");

            Schema schema = Schema.FromCache(typeof(TS));

            // find updated columns (not null)
            Dictionary<string, object> dic = new Dictionary<string, object>();
            foreach (MemberInfo mi in template.GetType().GetPublicMembers())
            {
                string name = mi.Name;
                object value = template.GetDuckValue(name);
                if (null != value)
                {
                    string cqlName = schema.NetName2ColumnDefs[name].CqlName;
                    dic.Add(cqlName, value);
                }
            }

            IUpdateBuilder builder = new UpdateBuilder();
            builder.Table = schema.Table;
            builder.Columns = dic.Keys.ToArray();
            builder.Values = Enumerable.Repeat("?", builder.Columns.Length).ToArray();
            if (null != where)
            {
                // find where columns (not null)
                List<string> wheres = new List<string>();
                foreach (MemberInfo mi in where.GetType().GetPublicMembers())
                {
                    string name = mi.Name;
                    object value = where.GetDuckValue(name);
                    if (null != value)
                    {
                        string cqlName = schema.NetName2ColumnDefs[name].CqlName;
                        dic.Add(cqlName, value);
                        wheres.Add(cqlName + "=?");
                    }
                }

                builder.Wheres = wheres.ToArray();
            }
            builder.ConsistencyLevel = @this.BehaviorConfig.WriteConsistencyLevel;
            builder.TTL = @this.BehaviorConfig.TTL;
            builder.Timestamp = @this.TimestampService.Generate();

            string cql = builder.Build();

            @this.ExecuteNonQuery(schema, cql, dic);
        }

        public static void CreateKeyspace<TS>(this ICluster @this, int replicationFactor) where TS : new()
        {
            Dictionary<string, int> replicationFactors = new Dictionary<string, int> {{"replication_factor", replicationFactor}};
            @this.CreateKeyspace<TS>("SimpleStrategy", replicationFactors);
        }

        public static void CreateKeyspace<TS>(this ICluster @this, string strategyClass, Dictionary<string, int> replicationFactor) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(TS));

            ICreateKeyspaceBuilder builder = new CreateKeyspaceBuilder();
            builder.Keyspace = schema.Keyspace;
            builder.StrategyClass = strategyClass;
            builder.ReplicationFactor = replicationFactor;
            string createKeyspaceStmt = builder.Build();

            @this.ExecuteCql(createKeyspaceStmt);
        }

        public static void DropKeyspace<TS>(this ICluster @this) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(TS));

            IDropKeyspaceBuilder builder = new DropKeyspaceBuilder();
            builder.Keyspace = schema.Keyspace;
            string dropKeyspaceStmt = builder.Build();

            @this.ExecuteCql(dropKeyspaceStmt);
        }

        public static void CreateTable<TS>(this ICluster @this) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(TS));

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

        public static void DropTable<TS>(this ICluster @this) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(TS));

            IDropTableBuilder builder = new DropTableBuilder();
            builder.Table = schema.Table;
            string dropTableStmt = builder.Build();

            @this.ExecuteCql(dropTableStmt);
        }

        public static void TruncateTable<TS>(this ICluster @this) where TS : new()
        {
            @this.CheckArgumentNotNull("@this");

            Schema schema = Schema.FromCache(typeof(TS));

            ITruncateTableBuilder tableBuilder = new TruncateTableBuilder();
            tableBuilder.Table = schema.Table;
            string dropTableStmt = tableBuilder.Build();

            @this.ExecuteCql(dropTableStmt);
        }
    }
}