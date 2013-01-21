using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CassandraSharp.Config;
using CassandraSharp.CQL;
using CassandraSharp.CQLPoco;
using CassandraSharp;
using System.Diagnostics;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;
using NUnit.Framework;

namespace CassandraSharpUnitTests.Performance
{
    [TestFixture]
    public class WritePerformanceTest
    {
        public const int NUM_ROUND = 5;
        public const int NUM_WRITES_PER_ROUND = 10000;

        [Test]
        public void runWritePerformanceSingleThread()
        {
            //run Write Performance Test using cassandra-sharp driver
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {"localhost"}
                            }
                };

            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
            {
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
                int n =0;
                while (n < NUM_ROUND)
                {
                    var timer = new Stopwatch();
                    timer.Start();

                    for (int i = 0; i < NUM_WRITES_PER_ROUND; i++)
                    {
                        prepared.ExecuteNonQuery(new { intid =i , strid = i.ToString("X")}).Wait();
                    }

                    timer.Stop();
                    double rate = (double) NUM_WRITES_PER_ROUND/ ((double)timer.ElapsedMilliseconds /1000d);
                    Console.WriteLine("[Cassandra-Sharp] Time : " + timer.ElapsedMilliseconds + " (rate: " + rate + " qps)");
                    n++;
                }

                Console.WriteLine("============================================================");
                Console.WriteLine(" Thrift Driver write performance test single thread ");
                Console.WriteLine("============================================================");

                TTransport transport = new TFramedTransport(new TSocket("localhost", 9160));
                TProtocol protocol = new TBinaryProtocol(transport);
                Cassandra.Client client = new Cassandra.Client(protocol);

                transport.Open();

                client.set_keyspace("tests");

                CqlPreparedResult query = client.prepare_cql_query(Encoding.ASCII.GetBytes("UPDATE tests.stresstest SET intid = ? WHERE strid = ?"), Compression.NONE);

                n = 0;
                while (n < NUM_ROUND)
                {
                    var timer = new Stopwatch();
                    timer.Start();

                    for (int i = 0; i < NUM_WRITES_PER_ROUND; i++)
                    {
                        client.execute_prepared_cql_query(query.ItemId, new List<byte[]> { BitConverter.GetBytes(i).Reverse().ToArray(), Encoding.ASCII.GetBytes(i.ToString("X")) });
                    }

                    timer.Stop();
                    double rate = (double)NUM_WRITES_PER_ROUND / ((double)timer.ElapsedMilliseconds / 1000d);
                    Console.WriteLine("[Cassandra-Thrift] Time : " + timer.ElapsedMilliseconds + " (rate: " + rate + " qps)");
                    n++;
                }
                const string dropFoo = "drop keyspace Tests";
                Console.WriteLine("============================================================");
                Console.WriteLine(dropFoo);
                Console.WriteLine("============================================================");
                
                resCount = cluster.ExecuteNonQuery(dropFoo);
                resCount.Wait();
            }


        }
    }
}
