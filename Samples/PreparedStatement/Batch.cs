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
    using System.Linq;
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
            using (var cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                const string createKeyspaceFoo = "CREATE KEYSPACE Foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                cluster.ExecuteNonQuery(createKeyspaceFoo)
                    .Wait();

                const string createBar = "CREATE TABLE Foo.Bar (id int, Baz blob, PRIMARY KEY (id))";
                cluster.ExecuteNonQuery(createBar)
                    .Wait();

                const string insertBatch = "INSERT INTO Foo.Bar (id, Baz) VALUES (?, ?)";
                var preparedInsert = cluster.Prepare(insertBatch);

                const int times = 10000;

                var random = new Random();

                for (int i = 0; i < times; i++)
                {
                    long running = Interlocked.Increment(ref _running);

                    Console.WriteLine("Current {0} Running {1}", i, running);

                    var data = new byte[30000];
                    // var data = (float)random.NextDouble();
                    preparedInsert.ExecuteNonQuery(new { id = i, Baz = data })
                        .ContinueWith(_ => Interlocked.Decrement(ref _running));
                }

                while (Thread.VolatileRead(ref _running) > 0)
                {
                    Console.WriteLine("Running {0}", _running);
                    Thread.Sleep(1000);
                }

                var result = cluster.Execute<Foo>("select * from Foo.Bar where id = 50").Result;
                foreach (var res in result)
                {
                    Console.WriteLine("{0} len={1}", res.Id, res.Baz.Length);
                }

                cluster.ExecuteNonQuery("drop keyspace Foo").Wait();
            }
            ClusterManager.Shutdown();
        }

        public class Foo
        {
            public int Id;

            public byte[] Baz;
        }
    }
}