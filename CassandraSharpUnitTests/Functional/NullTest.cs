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
    using System.Linq;
    using CassandraSharp;
    using CassandraSharp.CQLPropertyBag;
    using CassandraSharp.Config;
    using NUnit.Framework;

    [TestFixture]
    public class NullTest
    {
        [TearDown]
        public void TearDown()
        {
            ClusterManager.Shutdown();
        }

        [Test]
        public void TestNull()
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] { "localhost" }
                            }
                };

            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
            {
                var cmd = cluster.CreatePropertyBagCommand();

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

                cmd.Execute(createFoo).AsFuture().Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string createBar = @"CREATE TABLE Tests.AllTypes (a int, b int, primary key (a))";
                Console.WriteLine("============================================================");
                Console.WriteLine(createBar);
                Console.WriteLine("============================================================");
                cmd.Execute(createBar).AsFuture().Wait();
                Console.WriteLine();
                Console.WriteLine();

                //const string useBar = @"use Tests";
                //Console.WriteLine("============================================================");
                //Console.WriteLine(useBar);
                //Console.WriteLine("============================================================");
                //cmd.Execute(useBar).AsFuture().Wait();
                //Console.WriteLine();
                //Console.WriteLine();

                const string insertBatch = @"insert into Tests.AllTypes (a, b) values (?, ?)";
                var prepared = cmd.Prepare(insertBatch);

                PropertyBag insertBag = new PropertyBag();
                insertBag["a"] = 1;
                insertBag["b"] = null;

                prepared.Execute(insertBag).AsFuture().Wait();

                const string selectAll = "select * from Tests.AllTypes";
                var res = cmd.Execute(selectAll).AsFuture();
                Assert.IsTrue(1 == res.Result.Count);
                PropertyBag selectBag = res.Result.Single();
                Assert.IsTrue(selectBag.Keys.Contains("a"));
                Assert.IsTrue(1 == (int) selectBag["a"]);
                Assert.IsTrue(null == selectBag["b"]);
            }
        }
    }
}