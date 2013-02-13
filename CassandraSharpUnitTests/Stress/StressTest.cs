// cassandra-sharp - a .NET client for Apache Cassandra
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

namespace CassandraSharpUnitTests.Stress
{
    using System;
    using System.Threading;
    using CassandraSharp;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using NUnit.Framework;

    [TestFixture]
    public class ResilienceTest
    {
        public class ResilienceLogger : ILogger
        {
            public void Debug(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Info(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Warn(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Error(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Fatal(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public bool IsDebugEnabled()
            {
                return true;
            }
        }

        [Test]
        public void RecoveryTest()
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig
                {
                        Recovery = "Simple",
                        //Logger = typeof(ResilienceLogger).AssemblyQualifiedName,
                };
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {"localhost"},
                            },
                };

            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
            {
                ICqlCommand cmd = new PocoCommand(cluster);

                const string dropFoo = "drop keyspace data";
                try
                {
                    cmd.Execute(dropFoo).Wait();
                }
                catch
                {
                }

                const string createFoo = "CREATE KEYSPACE data WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                cmd.Execute(createFoo).Wait();

                const string createBar = "CREATE TABLE data.test (time text PRIMARY KEY)";
                cmd.Execute(createBar).Wait();

                for (int i = 0; i < 30; ++i)
                {
                    while (true)
                    {
                        Thread.Sleep(1000);
                        var now = DateTime.Now;
                        string insert = String.Format("insert into data.test(time) values ('{0}');", now);
                        Console.WriteLine("{0}) {1}", i, insert);

                        try
                        {
                            cmd.Execute(insert).Wait();
                            break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }

                ClusterManager.Shutdown();
            }
        }
    }
}