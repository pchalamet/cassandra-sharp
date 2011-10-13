namespace CassandraSharp.Commands
{
    using Apache.Cassandra;

    internal static class ColumnFamily
    {
        public static void Insert(Cassandra.Client client, string columnFamilyName, byte[] key, byte[] columnName, byte[] value,
                                  int ttl, ConsistencyLevel consistencyLevel)
        {
            ColumnParent columnParent = new ColumnParent
                                            {
                                                Column_family = columnFamilyName
                                            };
            Column column = new Column
                                {
                                    Name = columnName,
                                    Value = value,
                                    Ttl = ttl,
                                    Timestamp = Timestamp.Now
                                };

            client.insert(key, columnParent, column, consistencyLevel);
        }

        public static void Remove(Cassandra.Client client, string columnFamilyName, byte[] key, byte[] column, ConsistencyLevel consistencyLevel)
        {
            ColumnPath columnPath = new ColumnPath
                                        {
                                            Column_family = columnFamilyName,
                                            Column = column
                                        };

            client.remove(key, columnPath, Timestamp.Now, consistencyLevel);
        }

        public static ColumnOrSuperColumn Get(Cassandra.Client client, string columnFamilyName, byte[] key, byte[] column, ConsistencyLevel consistencyLevel)
        {
            ColumnPath columnPath = new ColumnPath
                                        {
                                            Column_family = columnFamilyName,
                                            Column = column
                                        };

            ColumnOrSuperColumn columnOrSuperColumn = client.get(key, columnPath, consistencyLevel);
            return columnOrSuperColumn;
        }

        public static void Truncate(Cassandra.Client client, string columnFamilyName)
        {
            client.truncate(columnFamilyName);
        }

        public static void IncrementCounter(Cassandra.Client client, string columnFamilyName, byte[] key, byte[] columnName, long value,
                                            ConsistencyLevel consistencyLevel)
        {
            ColumnParent columnParent = new ColumnParent
                                            {
                                                Column_family = columnFamilyName
                                            };
            CounterColumn counterColumn = new CounterColumn();
            counterColumn.Name = columnName;
            counterColumn.Value = value;

            client.add(key, columnParent, counterColumn, consistencyLevel);
        }
    }
}