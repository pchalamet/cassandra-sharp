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

namespace CassandraSharpUnitTests.Functional
{
    using System;
    using System.Text;
    using System.Threading;
    using CassandraSharp;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using NUnit.Framework;

    public class ConsoleDebugLogger : ILogger
    {
        public void Debug(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Info(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Warn(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Error(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Fatal(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Dispose()
        {
        }

        public bool IsDebugEnabled()
        {
            return true;
        }

        private static void Log(string format, object[] prms)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("LOG   {0} [{1}] - ", DateTime.Now, Thread.CurrentThread.ManagedThreadId);
            sb.AppendFormat(format, prms);
            Console.WriteLine(sb);
        }
    }

    [TestFixture]
    public class StreamStarvationTest
    {
        public const int NUM_UPDATES = 10;

        public const int NUM_THREADS = 1;

        public ConsoleDebugLogger logger = new ConsoleDebugLogger();

        /**
         * Reproducing the issue #29 in cassandra-sharp 
         * https://github.com/pchalamet/cassandra-sharp/issues/29
         * malformed prepared query cause issues to be swallwoed and hold indefinelty to stream_id.
         * */

        public void FailingThread(IPreparedQuery<NonQuery> prepared)
        {
            logger.Debug("Starting Failing thread ");
            Thread.Sleep(5000);

            for (int i = 0; i < NUM_UPDATES; i++)
            {
                logger.Debug("start fail update #" + i);
                try
                {
                    if (0 == (i % 2))
                    {
                        prepared.Execute(new { bar = "bar" + 1, strid = "1" }).AsFuture().Wait();
                        Assert.IsTrue(false, "Update should have failed");
                    }
                    else
                    {
                        prepared.Execute(new {bar = "bar" + 1, intid = i, strid = "1"}).AsFuture().Wait();
                        Console.WriteLine("Update {0} sucessful", i);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Update {0} failed with error {1}", i, ex.Message);
                }

                logger.Debug("end fail update #" + i);
                //Thread.Sleep(2000);
            }
        }

        [Test]
        public void StreamStarvationMultiThread()
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig
                {
                        Logger = new LoggerConfig {Type = typeof(ConsoleDebugLogger).AssemblyQualifiedName}
                };
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {
                                        new ServerConfig() {
                                            Server = "localhost"
                                        }
                                    }
                            },
                };

            ICluster cluster = ClusterManager.GetCluster(clusterConfig);
            ICqlCommand cmd = cluster.CreatePocoCommand();

            const string dropKeySpace = "drop keyspace Tests";
            try
            {
                cmd.Execute(dropKeySpace).AsFuture().Wait();
            }
            catch
            {
            }

            const string createKeySpace = "CREATE KEYSPACE Tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            Console.WriteLine("============================================================");
            Console.WriteLine(createKeySpace);
            Console.WriteLine("============================================================");

            cmd.Execute(createKeySpace).AsFuture().Wait();
            Console.WriteLine();
            Console.WriteLine();

            const string createFoo = "CREATE TABLE Tests.foo (strid varchar,bar varchar,intid int,PRIMARY KEY (strid))";
            Console.WriteLine("============================================================");
            Console.WriteLine(createFoo);
            Console.WriteLine("============================================================");
            cmd.Execute(createFoo).AsFuture().Wait();
            Console.WriteLine();
            Console.WriteLine();

            const string insertPerf = "UPDATE Tests.foo SET bar = ?, intid = ? WHERE strid = ?";
            Console.WriteLine("============================================================");
            Console.WriteLine(" Cassandra-Sharp Driver reproducing stream starvation ");
            Console.WriteLine("============================================================");

            var prepared = cmd.WithConsistencyLevel(ConsistencyLevel.ONE).Prepare(insertPerf);
            Thread[] failsThreads = new Thread[NUM_THREADS];

            for (int i = 0; i < NUM_THREADS; i++)
            {
                failsThreads[i] = new Thread(() => FailingThread(prepared));
                failsThreads[i].Start();
                //Thread.Sleep(5000);
            }

            foreach (Thread thread in failsThreads)
            {
                thread.Join();
            }

            ClusterManager.Shutdown();
        }
    }
}