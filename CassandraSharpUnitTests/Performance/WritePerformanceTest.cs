// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace CassandraSharpUnitTests.Performance
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading;
    using Apache.Cassandra;
    using CassandraSharp;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Cluster;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;
    using NUnit.Framework;
    using Thrift.Protocol;
    using Thrift.Transport;

    [TestFixture]
    public class WritePerformanceTest
    {
        public const int NUM_ROUND = 5;

        public const int NUM_WRITES_PER_ROUND = 10000;

        public class PerformanceInstrumentation : IInstrumentation
        {
            private readonly object _lock = new object();

            public static string RootFolder;

            private TextWriter _txtWriter;

            public PerformanceInstrumentation()
            {
                Guid guid = Guid.NewGuid();
                string filename = Path.Combine(RootFolder, guid.ToString()) + ".csv";
                _txtWriter = new StreamWriter(filename, false, Encoding.ASCII);
                _txtWriter.WriteLine("SessionId,Activity,EventId,Source,SourceElapsed,Thread");
            }

            public void ClientQuery(InstrumentationToken token)
            {
            }

            public void ClientConnectionInfo(InstrumentationToken token, IPAddress coordinator, byte streamId)
            {
            }

            public void ClientTrace(InstrumentationToken token, EventType eventType)
            {
            }

            public void ServerTrace(InstrumentationToken token, TracingSession tracingSession)
            {
                StringBuilder sb = new StringBuilder();
                foreach (TracingEvent te in tracingSession.TracingEvents)
                {
                    sb.AppendFormat("{0},{1},{2},{3},{4},{5}", tracingSession.SessionId, te.Activity, te.EventId, te.Source, te.SourceElapsed, te.Thread);
                    sb.AppendLine();
                }

                lock (_lock)
                    _txtWriter.Write(sb);
            }

            public void Dispose()
            {
                _txtWriter.Dispose();
                _txtWriter = null;
            }
        }

        [Test]
        public void BinaryProtocolRunWritePerformanceSingleThread()
        {
            PerformanceInstrumentation.RootFolder = "";

            //run Write Performance Test using cassandra-sharp driver
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            cassandraSharpConfig.Instrumentation = new InstrumentationConfig();
            cassandraSharpConfig.Instrumentation.Type = typeof(PerformanceInstrumentation).AssemblyQualifiedName;
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
                ICqlCommand cmd = new PocoCommand(cluster);

                const string dropFoo = "drop keyspace Tests";
                try
                {
                    cmd.Execute(dropFoo).AsFuture().Wait();
                }
                catch
                {
                }

                const string createFoo = "CREATE KEYSPACE Tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                Console.WriteLine("============================================================");
                Console.WriteLine(createFoo);
                Console.WriteLine("============================================================");

                var resCount = cmd.Execute(createFoo);
                resCount.AsFuture().Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string createBar = "CREATE TABLE Tests.stresstest (strid varchar,intid int,PRIMARY KEY (strid))";
                Console.WriteLine("============================================================");
                Console.WriteLine(createBar);
                Console.WriteLine("============================================================");
                resCount = cmd.Execute(createBar);
                resCount.AsFuture().Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string insertPerf = "UPDATE Tests.stresstest SET intid = ? WHERE strid = ?";
                Console.WriteLine("============================================================");
                Console.WriteLine(" Cassandra-Sharp Driver write performance test single thread ");
                Console.WriteLine("============================================================");
                var prepared = cmd.Prepare(insertPerf);
                int n = 0;
                while (n < NUM_ROUND)
                {
                    var timer = new Stopwatch();
                    timer.Start();

                    for (int i = 0; i < NUM_WRITES_PER_ROUND; i++)
                    {
                        prepared.Execute(new {intid = i, strid = i.ToString("X")}).AsFuture().Wait();
                    }

                    timer.Stop();
                    double rate = (1000.0*NUM_WRITES_PER_ROUND)/timer.ElapsedMilliseconds;
                    Console.WriteLine("[Cassandra-Sharp] Time : " + timer.ElapsedMilliseconds + " (rate: " + rate + " qps)");
                    n++;
                }

                Console.WriteLine("============================================================");
                Console.WriteLine(dropFoo);
                Console.WriteLine("============================================================");

                resCount = cmd.Execute(dropFoo);
                resCount.AsFuture().Wait();
            }

            Thread.Sleep(10*1000);

            ClusterManager.Shutdown();
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
                    CqlResult res = client.execute_prepared_cql3_query(query.ItemId,
                                                                       new List<byte[]>
                                                                           {
                                                                                   BitConverter.GetBytes(i).Reverse().ToArray(),
                                                                                   Encoding.ASCII.GetBytes(i.ToString("X"))
                                                                           },
                                                                       Apache.Cassandra.ConsistencyLevel.QUORUM);
                }

                timer.Stop();
                double rate = (1000.0*NUM_WRITES_PER_ROUND)/timer.ElapsedMilliseconds;
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