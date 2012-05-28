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

    public static class SchemaExtensions
    {
        public static void CreateTable<T>(this ICluster cluster)
        {
            Type type = typeof(T);

            SchemaAttribute schemaAttribute = type.FindSchemaAttribute();
            IEnumerable<ColumnDef> allColumns = type.FindColumns();
            string tableName = schemaAttribute.Name ?? type.Name;
            IEnumerable<ColumnDef> keyColumns = from cdef in allColumns
                                                where cdef.IsKeyComponent
                                                orderby cdef.Index
                                                select cdef;

            ICreateTableBuilder builder = new CreateTableBuilder();
            builder.Table = tableName;
            builder.Columns = allColumns.Select(x => x.Name).ToArray();
            builder.ColumnTypes = allColumns.Select(x => x.CqlType).ToArray();
            builder.Keys = keyColumns.Select(x => x.Name).ToArray();
            builder.CompactStorage = schemaAttribute.CompactStorage;
            string createTableStmt = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schemaAttribute.Keyspace};
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

        public static void DropTable<T>(this ICluster cluster)
        {
            Type t = typeof(T);

            SchemaAttribute schemaAttribute = t.FindSchemaAttribute();

            IDropTableBuilder builder = new DropTableBuilder();
            builder.Table = schemaAttribute.Name ?? t.Name;
            string dropTableStmt = builder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schemaAttribute.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                tmpCluster.ExecuteCql(dropTableStmt);
        }

        public static void Truncate<T>(this ICluster cluster)
        {
            Type t = typeof(T);

            SchemaAttribute schemaAttribute = t.FindSchemaAttribute();

            ITruncateTableBuilder tableBuilder = new TruncateTableBuilder();
            tableBuilder.Table = schemaAttribute.Name ?? t.Name;
            string dropTableStmt = tableBuilder.Build();

            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = schemaAttribute.Keyspace};
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
                tmpCluster.ExecuteCql(dropTableStmt);
        }
    }
}