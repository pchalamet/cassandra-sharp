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

namespace Samples.PreparedStatement
{
    using System;
    using System.Threading;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;

    public class Batch
    {
        private static long _running;

        public static void Run()
        {
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                const string createFoo = "CREATE KEYSPACE Foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                Console.WriteLine("============================================================");
                Console.WriteLine(createFoo);
                Console.WriteLine("============================================================");

                var resCount = cluster.ExecuteNonQuery(createFoo, ConsistencyLevel.QUORUM);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string createBar = "CREATE TABLE Foo.Bar (a int, " +
                                         "b int, " +
                                         "PRIMARY KEY (a, b))";
                Console.WriteLine("============================================================");
                Console.WriteLine(createBar);
                Console.WriteLine("============================================================");
                resCount = cluster.ExecuteNonQuery(createBar, ConsistencyLevel.QUORUM);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string insertBatch = "insert into Foo.Bar (a, b) values (?, ?)";
                var prepared = cluster.Prepare(insertBatch).Result;

                for (int aIdx = 0; aIdx < 100; ++aIdx)
                {
                    for (int bIdx = 0; bIdx < 1000; ++bIdx)
                    {
                        Interlocked.Increment(ref _running);
                        prepared.ExecuteNonQuery(ConsistencyLevel.ONE, new {a = aIdx, b = bIdx})
                                           .ContinueWith(_ => Interlocked.Decrement(ref _running));
                    }
                }

                while (0 < Thread.VolatileRead(ref _running))
                {
                    Thread.Sleep(1*1000);
                }

                const string select50 = "select * from Foo.Bar where a=50";
                foreach (Foo foo in cluster.Execute<Foo>(select50, ConsistencyLevel.ONE).Result)
                {
                    Console.WriteLine("a={0} b={1}", foo.a, foo.b);
                }

                const string dropFoo = "drop keyspace Foo";
                resCount = cluster.ExecuteNonQuery(dropFoo, ConsistencyLevel.ONE);
                resCount.Wait();
            }

            ClusterManager.Shutdown();
        }

        public class Foo
        {
            public int a;

            public int b;
        }
    }
}