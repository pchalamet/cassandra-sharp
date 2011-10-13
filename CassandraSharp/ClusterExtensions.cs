// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace CassandraSharp
{
    using System;
    using Apache.Cassandra;
    using CassandraSharp.Commands;

    public static class ClusterExtensions
    {
        public static void Insert(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column, INameOrValue value)
        {
            @this.Insert(columnFamily, key, column, value, @this.DefaultTTL, @this.DefaultWriteConsistencyLevel);
        }

        public static void Insert(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column, INameOrValue value, int ttl,
                                  ConsistencyLevel consistencyLevel)
        {
            if (null == @this || null == columnFamily || null == key || null == column)
            {
                throw new ArgumentNullException();
            }

            @this.Execute(ctx => ColumnFamily.Insert(ctx,
                                                     columnFamily,
                                                     key.ToByteArray(),
                                                     column.ToByteArray(),
                                                     value.ToByteArray(),
                                                     ttl,
                                                     consistencyLevel));
        }

        public static ColumnOrSuperColumn Get(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column)
        {
            return @this.Get(columnFamily, key, column, @this.DefaultReadConsistencyLevel);
        }

        public static ColumnOrSuperColumn Get(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column,
                                              ConsistencyLevel consistencyLevel)
        {
            if (null == @this || null == columnFamily || null == key || null == column)
            {
                throw new ArgumentNullException();
            }

            return @this.Execute(ctx => ColumnFamily.Get(ctx,
                                                         columnFamily,
                                                         key.ToByteArray(),
                                                         column.ToByteArray(),
                                                         consistencyLevel));
        }

        public static void Remove(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column)
        {
            @this.Remove(columnFamily, key, column, @this.DefaultWriteConsistencyLevel);
        }

        public static void Remove(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column,
                                  ConsistencyLevel consistencyLevel)
        {
            if (null == @this || null == columnFamily || null == key)
            {
                throw new ArgumentNullException();
            }

            @this.Execute(ctx => ColumnFamily.Remove(ctx,
                                                     columnFamily,
                                                     key.ToByteArray(),
                                                     null != column
                                                         ? column.ToByteArray()
                                                         : null,
                                                     consistencyLevel));
        }

        public static void Truncate(this ICluster @this, string columnFamily)
        {
            if (null == @this)
            {
                throw new ArgumentNullException();
            }

            @this.Execute(ctx => ColumnFamily.Truncate(ctx, columnFamily));
        }

        public static string DescribeClusterName(this ICluster @this)
        {
            if (null == @this)
            {
                throw new ArgumentNullException();
            }

            return @this.Execute(ctx => Describe.ClusterName(ctx));
        }

        public static string AddColumnFamily(this ICluster @this, CfDef cfDef)
        {
            if (null == @this || null == cfDef)
            {
                throw new ArgumentNullException();
            }

            return @this.Execute(ctx => SystemManagement.AddColumnFamily(ctx, cfDef));
        }

        public static string DropColumnFamily(this ICluster @this, string columnFamily)
        {
            if (null == @this || null == columnFamily)
            {
                throw new ArgumentNullException();
            }

            return @this.Execute(ctx => SystemManagement.DropColumnFamily(ctx, columnFamily));
        }

        public static CqlResult ExecuteCql(this ICluster @this, string cql)
        {
            if (null == @this || null == cql)
            {
                throw new ArgumentNullException();
            }

            return @this.Execute(ctx => Cql.Command(ctx, cql));
        }
    }
}