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
    using System.Text;
    using CassandraSharp.MadeSimple;

    public static class SchemaExtensions
    {
        public static void Create<T>(this ICluster cluster)
        {
            Type type = typeof(T);

            SchemaAttribute schemaAttribute = type.FindSchemaAttribute();
            IEnumerable<ColumnDef> allColumns = type.FindColumns();

            StringBuilder sbCreateTable = new StringBuilder("create table ");
            string tableName = schemaAttribute.Name ?? type.Name;

            // create table columns
            sbCreateTable.AppendFormat("{0} (", tableName);
            foreach (ColumnDef columnDef in allColumns)
            {
                string colDataType = columnDef.CqlType.ToCql();
                sbCreateTable.AppendFormat("{0} {1}, ", columnDef.Name, colDataType);
            }

            // add primary key definition
            IEnumerable<ColumnDef> keyColumns = from cdef in allColumns
                                                where cdef.IsKeyComponent
                                                orderby cdef.Index
                                                select cdef;

            sbCreateTable.AppendFormat("primary key (");
            string sep = "";
            foreach (ColumnDef cdef in keyColumns)
            {
                sbCreateTable.AppendFormat("{0}{1}", sep, cdef.Name);
                sep = ", ";
            }
            sbCreateTable.Append(") ) ");

            if (schemaAttribute.CompactStorage)
            {
                sbCreateTable.AppendFormat("WITH COMPACT STORAGE");
            }

            string createTableStmt = sbCreateTable.ToString();

            BehaviorConfigBuilder cfgBuilder = new BehaviorConfigBuilder();
            cfgBuilder.KeySpace = schemaAttribute.Keyspace;
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
            {
                tmpCluster.ExecuteCql(createTableStmt);
            }

            // create indices then
            //foreach (IndexAttribute ia in indices)
            //{
            //    StringBuilder sbCreateIndex = new StringBuilder();
            //    sbCreateIndex.AppendFormat("create index on '{0}'('{1}')", tableName, ia.Name);

            //    string createIndexStmt = sbCreateIndex.ToString();
            //    tmpCluster.ExecuteCql(createIndexStmt);
            //}
        }

        public static void Drop<T>(this ICluster cluster)
        {
            Type t = typeof(T);

            SchemaAttribute schemaAttribute = t.FindSchemaAttribute();

            StringBuilder sbDropTable = new StringBuilder();
            string tableName = schemaAttribute.Name ?? t.Name;
            sbDropTable.AppendFormat("drop columnfamily {0}", tableName);

            string dropTableStmt = sbDropTable.ToString();

            BehaviorConfigBuilder cfgBuilder = new BehaviorConfigBuilder { KeySpace = schemaAttribute.Keyspace };
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
            {
                tmpCluster.ExecuteCql(dropTableStmt);
            }
        }

        public static void Truncate<T>(this ICluster cluster)
        {
            Type t = typeof(T);

            SchemaAttribute schemaAttribute = t.FindSchemaAttribute();

            StringBuilder sbTruncateTable = new StringBuilder();
            string tableName = schemaAttribute.Name ?? t.Name;
            sbTruncateTable.AppendFormat("truncate '{0}'", tableName);

            string dropTableStmt = sbTruncateTable.ToString();

            BehaviorConfigBuilder cfgBuilder = new BehaviorConfigBuilder();
            cfgBuilder.KeySpace = schemaAttribute.Keyspace;
            using (ICluster tmpCluster = cluster.CreateChildCluster(cfgBuilder))
            {
                tmpCluster.ExecuteCql(dropTableStmt);
            }
        }
    }
}