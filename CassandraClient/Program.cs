namespace CassandraClient
{
    using System;
    using System.Text;
    using Apache.Cassandra;
    using CassandraSharp;
    using CassandraSharp.Commands;
    using CassandraSharp.Config;

    public class Program
    {
        public static void Main(string[] args)
        {
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                cluster.DefaultReadConsistencyLevel = ConsistencyLevel.ONE;
                cluster.DefaultWriteConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;

                string clusterName = cluster.Execute(Describe.ClusterName);
                Console.WriteLine(clusterName);

                //try
                //{
                //    cluster.DropKeySpace("TestKeySpace");
                //}
                //catch (InvalidRequestException)
                //{
                //}

                //KsDef ksDef = new KsDef
                //                  {
                //                      Name = "TestKeySpace",
                //                      Strategy_class = "org.apache.cassandra.locator.SimpleStrategy",
                //                      Strategy_options = new Dictionary<string, string> {{"replication_factor", "1"}}
                //                  };
                //ksDef.Cf_defs = new List<CfDef>();
                //cluster.AddKeySpace(ksDef);

                cluster.Keyspace = "TestKeySpace";

                //CfDef cfDef = new CfDef
                //                  {
                //                      Name = "TestCF",
                //                      Keyspace = "TestKeySpace"
                //                  };
                //cluster.AddColumnFamily(cfDef);

                const string columnName = "column";
                const string key = "key";
                byte[] value = Encoding.UTF8.GetBytes("tralala");
                cluster.Insert("TestCF", key, columnName, value, ConsistencyLevel.LOCAL_QUORUM);

                ColumnOrSuperColumn cosc = cluster.Get("TestCF", key, columnName);
                string result = Encoding.UTF8.GetString(cosc.Column.Value);
                Console.WriteLine(result);

                //cluster.Truncate("TestCF");
            }

            ClusterManager.Shutdown();
        }
    }
}