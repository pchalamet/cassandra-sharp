namespace CassandraSharp
{
    using System;
    using Apache.Cassandra;
    using CassandraSharp.Commands;

    public static class ClusterExtensions
    {
        public static void SetKeySpace(this ICluster @this, string keyspace)
        {
            @this.Execute(null, ctx => SystemManagement.SetKeySpace(ctx, keyspace));
        }

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

            @this.Execute(null,
                          ctx => ColumnFamily.Insert(ctx,
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

            return @this.ExecuteCommand(null,
                                 ctx => ColumnFamily.Get(ctx,
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

            @this.Execute(null,
                          ctx => ColumnFamily.Remove(ctx,
                                                     columnFamily,
                                                     key.ToByteArray(),
                                                     null != column
                                                         ? column.ToByteArray()
                                                         : null,
                                                     consistencyLevel));
        }

        public static void IncrementCounter(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column, long value)
        {
            @this.IncrementCounter(columnFamily, key, column, value, @this.DefaultWriteConsistencyLevel);
        }

        public static void IncrementCounter(this ICluster @this, string columnFamily, INameOrValue key, INameOrValue column, long value,
                                            ConsistencyLevel consistencyLevel)
        {
            @this.Execute(null,
                          ctx => ColumnFamily.IncrementCounter(ctx, columnFamily, key.ToByteArray(), column.ToByteArray(), value, consistencyLevel));
        }

        public static void Truncate(this ICluster @this, string columnFamily)
        {
            if (null == @this)
            {
                throw new ArgumentNullException();
            }

            @this.Execute(null, ctx => ColumnFamily.Truncate(ctx, columnFamily));
        }

        public static string DescribeClusterName(this ICluster @this)
        {
            if (null == @this)
            {
                throw new ArgumentNullException();
            }

            return @this.ExecuteCommand(null, ctx => Describe.ClusterName(ctx));
        }

        public static string AddColumnFamily(this ICluster @this, CfDef cfDef)
        {
            if (null == @this || null == cfDef)
            {
                throw new ArgumentNullException();
            }

            return @this.ExecuteCommand(null, ctx => SystemManagement.AddColumnFamily(ctx, cfDef));
        }

        public static string DropColumnFamily(this ICluster @this, string columnFamily)
        {
            if (null == @this || null == columnFamily)
            {
                throw new ArgumentNullException();
            }

            return @this.ExecuteCommand(null, ctx => SystemManagement.DropColumnFamily(ctx, columnFamily));
        }

        public static CqlResult ExecuteCql(this ICluster @this, string cql)
        {
            if (null == @this || null == cql)
            {
                throw new ArgumentNullException();
            }

            return @this.ExecuteCommand(null, ctx => Cql.Command(ctx, cql));
        }
    }
}