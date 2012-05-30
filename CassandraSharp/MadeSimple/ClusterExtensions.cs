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

namespace CassandraSharp.MadeSimple
{
    using Apache.Cassandra;
    using CassandraSharp.Utils;

    public static class ClusterExtensions
    {
        private static ConsistencyLevel Get(this ConsistencyLevel? @this)
        {
            return @this ?? ConsistencyLevel.QUORUM;
        }

        public static void Insert(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue columnName, INameOrValue value)
        {
            @this.CheckArgumentNotNull("@this");
            columnFamily.CheckArgumentNotNull("columnFamily");
            key.CheckArgumentNotNull("key");
            columnName.CheckArgumentNotNull("columnName");

            ColumnParent columnParent = new ColumnParent
                                            {
                                                Column_family = columnFamily
                                            };

            Column column = new Column
                                {
                                    Name = columnName.ConvertToByteArray(),
                                    Value = value.ConvertToByteArray(),
                                    Timestamp = @this.TimestampService.Generate()
                                };

            @this.Execute(cnx => cnx.CassandraClient.insert(key.ConvertToByteArray(), columnParent, column, @this.BehaviorConfig.WriteConsistencyLevel.Get()));
        }

        public static ColumnOrSuperColumn Get(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue columnName)
        {
            @this.CheckArgumentNotNull("@this");
            columnFamily.CheckArgumentNotNull("columnFamily");
            key.CheckArgumentNotNull("key");
            columnName.CheckArgumentNotNull("columnName");

            ColumnPath columnPath = new ColumnPath
                                        {
                                            Column_family = columnFamily,
                                            Column = columnName.ConvertToByteArray()
                                        };

            return @this.ExecuteCommand(cnx => cnx.CassandraClient.get(key.ConvertToByteArray(), columnPath, @this.BehaviorConfig.ReadConsistencyLevel.Get()));
        }

        public static void Remove(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column)
        {
            @this.CheckArgumentNotNull("@this");
            columnFamily.CheckArgumentNotNull("columnFamily");
            key.CheckArgumentNotNull("key");
            column.CheckArgumentNotNull("columnName");

            ColumnPath columnPath = new ColumnPath
                                        {
                                            Column_family = columnFamily,
                                            Column = column.ConvertToByteArray()
                                        };

            @this.Execute(cnx =>
                          cnx.CassandraClient.remove(key.ConvertToByteArray(), columnPath, @this.TimestampService.Generate(),
                                                     @this.BehaviorConfig.WriteConsistencyLevel.Get()));
        }

        public static void IncrementCounter(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue columnName, long value)
        {
            @this.CheckArgumentNotNull("@this");
            columnFamily.CheckArgumentNotNull("columnFamily");
            key.CheckArgumentNotNull("key");

            ColumnParent columnParent = new ColumnParent
                                            {
                                                Column_family = columnFamily
                                            };
            CounterColumn counterColumn = new CounterColumn();
            counterColumn.Name = columnName.ConvertToByteArray();
            counterColumn.Value = value;

            @this.Execute(
                cnx => cnx.CassandraClient.add(key.ConvertToByteArray(), columnParent, counterColumn, @this.BehaviorConfig.WriteConsistencyLevel.Get()));
        }

        public static void CreateKeyspace(this ICluster @this, string name)
        {
            @this.CheckArgumentNotNull("@this");
        }

        public static void Truncate(this ICluster @this, string columnFamily)
        {
            @this.CheckArgumentNotNull("@this");
            columnFamily.CheckArgumentNotNull("columnFamily");

            @this.Execute(cnx => cnx.CassandraClient.truncate(columnFamily));
        }

        public static string DescribeClusterName(this ICluster @this)
        {
            @this.CheckArgumentNotNull("@this");

            return @this.ExecuteCommand(cnx => cnx.CassandraClient.describe_cluster_name());
        }

        public static CqlResult ExecuteCql(this ICluster @this, string cql)
        {
            @this.CheckArgumentNotNull("@this");
            cql.CheckArgumentNotNull("cql");

            byte[] query = new Utf8NameOrValue(cql).ConvertToByteArray();
            return @this.ExecuteCommand(ctx => ctx.CassandraClient.execute_cql_query(query, Compression.NONE));
        }
    }
}