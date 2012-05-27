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
        public static IEnumerable<T> Select<T>(this ICluster cluster, T template) where T : new()
        {
            Type type = typeof(T);

            SchemaAttribute schemaAttribute = type.FindSchemaAttribute();
            string tableName = schemaAttribute.Name ?? type.Name;
            IEnumerable<ColumnDef> allColumns = type.FindColumns();
            var selectors = from columnDef in allColumns
                            let value = columnDef.GetValue(template)
                            where null == value
                            select columnDef.Name;
            var wheres = from columnDef in allColumns
                         let value = columnDef.GetValue(template)
                         where null != value
                         select columnDef.Name + "=?";

            IQueryBuilder builder = new QueryBuilder();
            builder.Columns = selectors.ToArray();
            builder.Table = tableName;
            builder.ConsistencyLevel = cluster.BehaviorConfig.WriteConsistencyLevel;
            builder.Wheres = wheres.ToArray();
            string cqlSelect = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schemaAttribute.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                return tmpCluster.Execute<T>(cqlSelect, template);
        }

        public static void Insert<T>(this ICluster cluster, T param)
        {
            Type type = typeof(T);

            SchemaAttribute schemaAttribute = type.FindSchemaAttribute();
            IEnumerable<ColumnDef> allColumns = type.FindColumns();
            var selectors = from columnDef in allColumns
                            let value = columnDef.GetValue(param)
                            where null != value
                            select new {columnDef.Name, Setter = "?"};

            IInsertBuilder builder = new InsertBuilder();
            builder.Table = schemaAttribute.Name ?? type.Name;
            builder.Columns = selectors.Select(x => x.Name).ToArray();
            builder.Values = selectors.Select(x => x.Setter).ToArray();
            builder.ConsistencyLevel = cluster.BehaviorConfig.WriteConsistencyLevel;
            builder.TTL = cluster.BehaviorConfig.TTL;
            builder.Timestamp = cluster.TimestampService.Generate();
            string cqlInsert = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schemaAttribute.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                tmpCluster.ExecuteNonQuery(cqlInsert, param);
        }

        public static void Delete<T>(this ICluster cluster, T template)
        {
            // translate to "delete"
            throw new NotImplementedException();
        }

        public static void Update<T>(this ICluster cluster, T template, T param)
        {
            // translate to "update"
            throw new NotImplementedException();
        }
    }
}