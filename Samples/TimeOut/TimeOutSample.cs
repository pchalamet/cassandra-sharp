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

namespace Samples.TimeOut
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQLPoco;

    public class SchemaKeyspaces
    {
        public bool DurableWrites { get; set; }

        public string KeyspaceName { get; set; }

        // ReSharper disable InconsistentNaming
        public string strategy_Class { get; set; }

        // ReSharper restore InconsistentNaming

        // ReSharper disable InconsistentNaming
        public string strategy_options { get; set; }

        // ReSharper restore InconsistentNaming
    }

    public class TimeOutSample : Sample
    {
        public TimeOutSample()
                : base("TimeOutSample")
        {
        }

        protected override void InternalRun(ICluster cluster)
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();

            const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";

            Random rnd = new Random();
            for (int i = 0; i < 10; ++i)
            {
                DateTime dtStart = DateTime.Now;
                DateTime dtStop = dtStart.AddSeconds(2); // 2 second max
                int wait = rnd.Next(4*1000);
                var futRes = cmd.Execute<SchemaKeyspaces>(cqlKeyspaces)
                                    .ContinueWith(t =>
                                        {
                                            // simulate an eventually long operation
                                            Thread.Sleep(wait);
                                            return t;
                                        }).Unwrap().ContinueWith(t => DisplayKeyspace(t.Result, dtStop));
                futRes.Wait();
            }
        }

        private static void DisplayKeyspace(IEnumerable<SchemaKeyspaces> result, DateTime dtStop)
        {
            try
            {
                foreach (var resKeyspace in result)
                {
                    // if it's too late do not process the query
                    DateTime now = DateTime.Now;
                    if (now > dtStop)
                    {
                        Console.WriteLine("Query timeout {0} > {1}", now, dtStop);
                        throw new TimeoutException();
                    }

                    Console.WriteLine("DurableWrites={0} KeyspaceName={1} strategy_Class={2} strategy_options={3}",
                                      resKeyspace.DurableWrites, resKeyspace.KeyspaceName, resKeyspace.strategy_Class, resKeyspace.strategy_options);
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