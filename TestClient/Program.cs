namespace TestClient
{
    using System;
    using CassandraSharp;
    using CassandraSharp.Config;
    using CassandraSharp.Model;

    internal class Program
    {
        private static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            // using declarative configuration
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
                TestCluster(cluster);

            // using minimal declarative configuration
            // no keyspace is specified : we need to set IConnectionInfo before calling the command
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandraMinimal"))
            {
                IConnectionInfo cnxInfo = new ConnectionInfo("TestKS");
                ICluster overrideCluster = cnxInfo.Overrides(cluster);
                TestCluster(overrideCluster);
            }

            ClusterManager.Shutdown();
        }

        private static void TestCluster(ICluster cluster)
        {
            string clusterName = cluster.DescribeClusterName();
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