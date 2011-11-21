namespace TestClient
{
    using System;
    using CassandraSharp;
    using CassandraSharp.Commands;
    using CassandraSharp.Config;
    using CassandraSharp.Model;

    internal class Program
    {
        private static void Main(string[] args)
        {
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                TestCluster(cluster);
            }

            using (ICluster cluster = ClusterManager.GetCluster("TestCassandraMinimal"))
            {
                TestCluster(cluster);
            }

            ClusterManager.Shutdown();
        }

        private static void TestCluster(ICluster cluster)
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
    }
}