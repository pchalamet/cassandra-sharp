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
            // no keyspace is specified : we need to set ICommandInfo before calling the command
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandraMinimal"))
            {
                BehaviorConfigBuilder cmdInfoBuilder = new BehaviorConfigBuilder { KeySpace = "TestKS" };
                ICluster configuredCluster = cluster.Configure(cmdInfoBuilder);
                TestCluster(configuredCluster);
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