namespace TestClient
{
    using System;
    using Apache.Cassandra;
    using CassandraSharp;
    using CassandraSharp.Commands;
    using CassandraSharp.Config;
    using CassandraSharp.Model;

    internal class Program
    {
        private static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            // using declarative configuration
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                string clusterName = cluster.Execute(Describe.ClusterName);
                Console.WriteLine(clusterName);

                cluster.Truncate("CF");

                var nvColumn = new Utf8NameOrValue("column");
                var nvKey = new Utf8NameOrValue("key");
                var nvValue = new ByteArrayNameOrValue(new byte[10]);
                cluster.Insert(columnFamily: "CF",
                               key: nvKey,
                               column: nvColumn,
                               value: nvValue);
            }

            // unfortunately if you do not specify a default keyspace 
            // it is a bit cumbesome to use... this will be fixed in coming versions
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandraMinimal"))
            {
                const string keyspace = "TestKS";

                string clusterName = null;
                cluster.Execute(ctx =>
                                    {
                                        SystemManagement.SetKeySpace(ctx, keyspace);
                                        clusterName = Describe.ClusterName(ctx);
                                    });
                Console.WriteLine(clusterName);

                cluster.Execute(ctx =>
                                    {
                                        SystemManagement.SetKeySpace(ctx, keyspace);
                                        ColumnFamily.Truncate(ctx, "CF");
                                    });

                var nvColumn = new Utf8NameOrValue("column");
                var nvKey = new Utf8NameOrValue("key");
                var nvValue = new ByteArrayNameOrValue(new byte[10]);
                cluster.Execute(ctx =>
                                    {
                                        SystemManagement.SetKeySpace(ctx, keyspace);
                                        ColumnFamily.Insert(ctx, "CF",
                                                            nvKey.ToByteArray(),
                                                            nvColumn.ToByteArray(),
                                                            nvValue.ToByteArray(),
                                                            0,
                                                            ConsistencyLevel.QUORUM);
                                    });
            }

            ClusterManager.Shutdown();
        }
    }
}