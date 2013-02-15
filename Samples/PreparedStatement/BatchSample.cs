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

    public class BatchSample : Sample
    {
        private long _running;

        public BatchSample()
                : base("BatchSample")
        {
        }

        protected override void CreateKeyspace(ICluster cluster)
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();

            const string createKeyspaceFoo = "CREATE KEYSPACE Foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            cmd.Execute(createKeyspaceFoo).AsFuture().Wait();

            const string createBar = "CREATE TABLE Foo.Bar (id int, Baz blob, PRIMARY KEY (id))";
            cmd.Execute(createBar).AsFuture().Wait();
        }

        protected override void DropKeyspace(ICluster cluster)
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();
            cmd.Execute("drop keyspace Foo").AsFuture().Wait();
        }

        protected override void InternalRun(ICluster cluster)
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();

            const string insertBatch = "INSERT INTO Foo.Bar (id, Baz) VALUES (?, ?)";
            var preparedInsert = cmd.Prepare(insertBatch);

            const int times = 10;

            var random = new Random();

            for (int i = 0; i < times; i++)
            {
                long running = Interlocked.Increment(ref _running);

                Console.WriteLine("Current {0} Running {1}", i, running);

                var data = new byte[30000];
                // var data = (float)random.NextDouble();
                preparedInsert.Execute(new { id = i, Baz = data }, ConsistencyLevel.ONE).AsFuture()
                              .ContinueWith(_ => Interlocked.Decrement(ref _running));
            }

            while (Thread.VolatileRead(ref _running) > 0)
            {
                Console.WriteLine("Running {0}", _running);
                Thread.Sleep(1000);
            }

            var result = cmd.Execute<Foo>("select * from Foo.Bar where id = 50").AsFuture().Result;
            foreach (var res in result)
            {
                Console.WriteLine("{0} len={1}", res.Id, res.Baz.Length);
            }
        }

        public class Foo
        {
            public byte[] Baz;

            public int Id;
        }
    }
}