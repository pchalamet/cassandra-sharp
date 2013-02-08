namespace CassandraSharpUnitTests.Performance
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using Apache.Cassandra;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;
    using NUnit.Framework;
    using Thrift.Protocol;
    using Thrift.Transport;

    [TestFixture]
    public class WritePerformanceTest
    {
        public const int NUM_ROUND = 5;

        public const int NUM_WRITES_PER_ROUND = 10000;

        [Test]
        public void BinaryProtocolRunWritePerformanceSingleThread()
        {
            //run Write Performance Test using cassandra-sharp driver
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {"localhost"}
                            },
                };

            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
            {
                const string dropFoo = "drop keyspace Tests";
                try
                {
                    cluster.ExecuteNonQuery(dropFoo).Wait();
                }
                catch
                {
                }

                const string createFoo = "CREATE KEYSPACE Tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                Console.WriteLine("============================================================");
                Console.WriteLine(createFoo);
                Console.WriteLine("============================================================");

                var resCount = cluster.ExecuteNonQuery(createFoo);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string createBar = "CREATE TABLE Tests.stresstest (strid varchar,intid int,PRIMARY KEY (strid))";
                Console.WriteLine("============================================================");
                Console.WriteLine(createBar);
                Console.WriteLine("============================================================");
                resCount = cluster.ExecuteNonQuery(createBar);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string insertPerf = "UPDATE Tests.stresstest SET intid = ? WHERE strid = ?";
                Console.WriteLine("============================================================");
                Console.WriteLine(" Cassandra-Sharp Driver write performance test single thread ");
                Console.WriteLine("============================================================");
                var prepared = cluster.Prepare(insertPerf);
                int n = 0;
                while (n < NUM_ROUND)
                {
                    var timer = new Stopwatch();
                    timer.Start();

                    for (int i = 0; i < NUM_WRITES_PER_ROUND; i++)
                    {
                        prepared.ExecuteNonQuery(new {intid = i, strid = i.ToString("X")}).Wait();
                    }

                    timer.Stop();
                    double rate = NUM_WRITES_PER_ROUND/(timer.ElapsedMilliseconds/1000d);
                    Console.WriteLine("[Cassandra-Sharp] Time : " + timer.ElapsedMilliseconds + " (rate: " + rate + " qps)");
                    n++;
                }

                Console.WriteLine("============================================================");
                Console.WriteLine(dropFoo);
                Console.WriteLine("============================================================");

                resCount = cluster.ExecuteNonQuery(dropFoo);
                resCount.Wait();
            }
        }

        [Test]
        public void ThriftRunWritePerformanceSingleThread()
        {
            Console.WriteLine("============================================================");
            Console.WriteLine(" Thrift Driver write performance test single thread ");
            Console.WriteLine("============================================================");

            TTransport transport = new TFramedTransport(new TSocket("localhost", 9160));
            TProtocol protocol = new TBinaryProtocol(transport);
            Cassandra.Client client = new Cassandra.Client(protocol);

            transport.Open();

            const string dropFoo = "drop keyspace Tests";
            try
            {
                client.execute_cql3_query(Encoding.UTF8.GetBytes(dropFoo), Compression.NONE, Apache.Cassandra.ConsistencyLevel.QUORUM);
            }
            catch
            {
            }

            const string createFoo = "CREATE KEYSPACE Tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            Console.WriteLine("============================================================");
            Console.WriteLine(createFoo);
            Console.WriteLine("============================================================");

            client.execute_cql3_query(Encoding.UTF8.GetBytes(createFoo),
                                      Compression.NONE, Apache.Cassandra.ConsistencyLevel.QUORUM);
            Console.WriteLine();
            Console.WriteLine();

            const string createBar = "CREATE TABLE Tests.stresstest (strid varchar,intid int,PRIMARY KEY (strid))";
            Console.WriteLine("============================================================");
            Console.WriteLine(createBar);
            Console.WriteLine("============================================================"); 

            client.execute_cql3_query(Encoding.UTF8.GetBytes(createBar), Compression.NONE,
                                      Apache.Cassandra.ConsistencyLevel.QUORUM);
            Console.WriteLine();
            Console.WriteLine();

            CqlPreparedResult query = client.prepare_cql3_query(Encoding.UTF8.GetBytes("UPDATE tests.stresstest SET intid = ? WHERE strid = ?"),
                                                                Compression.NONE);

            int n = 0;
            while (n < NUM_ROUND)
            {
                var timer = new Stopwatch();
                timer.Start();

                for (int i = 0; i < NUM_WRITES_PER_ROUND; i++)
                {
                    client.execute_prepared_cql3_query(query.ItemId,
                                                      new List<byte[]> {BitConverter.GetBytes(i).Reverse().ToArray(), Encoding.ASCII.GetBytes(i.ToString("X"))}, 
                                                      Apache.Cassandra.ConsistencyLevel.QUORUM);
                }

                timer.Stop();
                double rate = NUM_WRITES_PER_ROUND/(timer.ElapsedMilliseconds/1000d);
                Console.WriteLine("[Cassandra-Thrift] Time : " + timer.ElapsedMilliseconds + " (rate: " + rate + " qps)");
                n++;
            }

            Console.WriteLine("============================================================");
            Console.WriteLine(dropFoo);
            Console.WriteLine("============================================================");
            client.execute_cql3_query(Encoding.UTF8.GetBytes(dropFoo), Compression.NONE, Apache.Cassandra.ConsistencyLevel.QUORUM);
        }
    }
}