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
    using System.Text;

    public static class MapperExtensions
    {
        private static void AppendQueryModifiers(ICluster cluster, StringBuilder sbQuery)
        {
            sbQuery.AppendFormat(" using consistency {0} and timestamp {1}",
                                 cluster.BehaviorConfig.WriteConsistencyLevel, cluster.TimestampService.Generate());
            if (0 != cluster.BehaviorConfig.TTL)
            {
                sbQuery.AppendFormat(" and ttl {0}", cluster.BehaviorConfig.TTL);
            }
        }

        public static IEnumerable<T> Read<T>(this ICluster cluster, T template)
        {
            // translate to "select"
            throw new NotImplementedException();
        }

        public static void Write<T>(this ICluster cluster, T param)
        {
            Type type = typeof(T);

            SchemaAttribute schemaAttribute = type.FindSchemaAttribute();
            string tableName = schemaAttribute.Name ?? type.Name;

            IEnumerable<ColumnDef> allColumns = type.FindColumns();

            StringBuilder sbInsert = new StringBuilder("insert into ");
            StringBuilder sbJokers = new StringBuilder("(");
            sbInsert.AppendFormat("{0} (", tableName);
            string sep = "";
            foreach (ColumnDef columnDef in allColumns)
            {
                object value = columnDef.GetValue(param);
                if (null != value)
                {
                    sbInsert.Append(sep).Append(columnDef.Name);
                    sbJokers.Append(sep).Append("?");
                    sep = ", ";
                }
            }
            sbInsert.Append(" ) values ").Append(sbJokers).Append(" )");
            AppendQueryModifiers(cluster, sbInsert);

            string cqlInsert = sbInsert.ToString();
            cluster.Execute(cqlInsert, param);
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