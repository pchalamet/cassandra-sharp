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

namespace CassandraSharp.MadeSimple
{
    using System;
    using Apache.Cassandra;

    public static class ClusterExtensions
    {
        public static void Insert(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue columnName, INameOrValue value)
        {
            if (null == @this || null == columnFamily || null == key || null == columnName)
            {
                throw new ArgumentNullException();
            }

            ColumnParent columnParent = new ColumnParent
                                            {
                                                Column_family = columnFamily
                                            };

            Column column = new Column
                                {
                                    Name = columnName.ToByteArray(),
                                    Value = value.ToByteArray(),
                                    Timestamp = @this.TimestampService.Generate()
                                };

            @this.Execute(cnx => cnx.CassandraClient.insert(key.ToByteArray(), columnParent, column, @this.BehaviorConfig.WriteConsistencyLevel));
        }

        public static ColumnOrSuperColumn Get(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column)
        {
            if (null == @this || null == columnFamily || null == key || null == column)
            {
                throw new ArgumentNullException();
            }

            ColumnPath columnPath = new ColumnPath
                                        {
                                            Column_family = columnFamily,
                                            Column = column.ToByteArray()
                                        };

            return @this.ExecuteCommand(cnx => cnx.CassandraClient.get(key.ToByteArray(), columnPath, @this.BehaviorConfig.ReadConsistencyLevel));
        }

        public static void Remove(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column)
        {
            if (null == @this || null == columnFamily || null == key)
            {
                throw new ArgumentNullException();
            }

            ColumnPath columnPath = new ColumnPath
                                        {
                                            Column_family = columnFamily,
                                            Column = column.ToByteArray()
                                        };

            @this.Execute(cnx =>
                          cnx.CassandraClient.remove(key.ToByteArray(), columnPath, @this.TimestampService.Generate(),
                                                     @this.BehaviorConfig.WriteConsistencyLevel));
        }

        public static void IncrementCounter(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue columnName, long value)
        {
            ColumnParent columnParent = new ColumnParent
                                            {
                                                Column_family = columnFamily
                                            };
            CounterColumn counterColumn = new CounterColumn();
            counterColumn.Name = columnName.ToByteArray();
            counterColumn.Value = value;

            @this.Execute(cnx => cnx.CassandraClient.add(key.ToByteArray(), columnParent, counterColumn, @this.BehaviorConfig.WriteConsistencyLevel));
        }

        public static void Truncate(this ICluster @this, string columnFamily)
        {
            if (null == @this)
            {
                throw new ArgumentNullException();
            }

            @this.Execute(cnx => cnx.CassandraClient.truncate(columnFamily));
        }

        public static string DescribeClusterName(this ICluster @this)
        {
            if (null == @this)
            {
                throw new ArgumentNullException();
            }

            return @this.ExecuteCommand(cnx => cnx.CassandraClient.describe_cluster_name());
        }

        public static CqlResult ExecuteCql(this ICluster @this, string cql)
        {
            if (null == @this || null == cql)
            {
                throw new ArgumentNullException();
            }

            byte[] query = new Utf8NameOrValue(cql).ToByteArray();
            return @this.ExecuteCommand(ctx => ctx.CassandraClient.execute_cql_query(query, Compression.NONE));
        }
    }
}