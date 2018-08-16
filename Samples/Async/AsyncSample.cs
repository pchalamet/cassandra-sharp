// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
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

namespace Samples.Async
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQLPoco;

    public class SchemaKeyspaces
    {
        public string KeyspaceName { get; set; }
        public bool DurableWrites { get; set; }
        public Dictionary<string, string> Replication { get; set; }
    }

    public class AsyncSample : Sample
    {
        public AsyncSample()
                : base("AsyncSample")
        {
        }

        protected override void InternalRun(ICluster cluster)
        {
            const string cqlKeyspaces = "SELECT * from system_schema.keyspaces";

            ICqlCommand cmd = cluster.CreatePocoCommand();

            var allTasks = new List<Task>();
            for (int i = 0; i < 100; ++i)
            {
                var futRes = cmd.Execute<SchemaKeyspaces>(cqlKeyspaces).AsFuture().ContinueWith(t => DisplayKeyspace(t.Result));
                allTasks.Add(futRes);
            }

            Task.WaitAll(allTasks.ToArray());
        }

        private static void DisplayKeyspace(IEnumerable<SchemaKeyspaces> result)
        {
            try
            {
                foreach (var resKeyspace in result)
                {
                    Console.WriteLine("DurableWrites={0} KeyspaceName={1} Class={2}",
                                      resKeyspace.DurableWrites, resKeyspace.KeyspaceName, 
                                      resKeyspace.Replication["class"]);
                }
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Command failed {0}", ex.Message);
            }
        }
    }
}