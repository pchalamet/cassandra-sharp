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
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using CassandraSharp.Extensibility;
    using NUnit.Framework;

    [TestFixture]
    public class WritePerformanceTest
    {
        public const int NUM_ROUND = 5;

        public const int NUM_WRITES_PER_ROUND = 10000;

        private static void RunWritePerformanceSingleThread<TP>() where TP : ProtocolWrapper, new()
        {
            using (ProtocolWrapper protocol = new TP())
            {
                protocol.Open("localhost");

                const string dropKeyspace = "drop keyspace Tests";
                const string truncateTable = "truncate Tests.stresstest";
                const string truncateEvents = "truncate system_traces.events";
                const string truncateSessions = "truncate system_traces.sessions";
                const string createKeyspace = "create keyspace Tests with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                const string createTable = "create table Tests.stresstest (strid varchar,intid int, primary key (strid))";
                const string insertPerf = "insert into Tests.stresstest (intid, strid) values (?, ?)";

                try
                {
                    protocol.Query(dropKeyspace);
                }
                // ReSharper disable EmptyGeneralCatchClause
                catch
                // ReSharper restore EmptyGeneralCatchClause
                {
                }

                protocol.Query(createKeyspace);
                protocol.Query(createTable);
                protocol.Query(truncateTable);
                protocol.Query(truncateSessions);
                protocol.Query(truncateEvents);
                protocol.Prepare(insertPerf);

                PerformanceInstrumentation.Initialize();

                ExecuteStressTest(protocol);

                ExportTracingInfo(protocol);

                protocol.Query(dropKeyspace);
            }
        }

        private static void ExecuteStressTest(ProtocolWrapper protocol)
        {
            int n = 0;
            while (n < NUM_ROUND)
            {
                var timer = new Stopwatch();
                timer.Start();

                for (int i = 0; i < NUM_WRITES_PER_ROUND; ++i)
                {
                    int key = n * NUM_WRITES_PER_ROUND + i;
                    object[] prms = new object[] { key, key.ToString("X") };
                    protocol.Execute(prms);
                }

                timer.Stop();
                double rate = (1000.0 * NUM_WRITES_PER_ROUND) / timer.ElapsedMilliseconds;

                Console.WriteLine("[{0} Time: {1} ms (rate: {2})", protocol.Name, timer.ElapsedMilliseconds, rate);
                ++n;
            }
        }

        private static void ExportTracingInfo(ProtocolWrapper protocol)
        {
            Console.WriteLine("Exporting performance for {0}", protocol.Name);
            Guid guid = Guid.NewGuid();
            string filename = protocol.Name + "-" + guid + ".csv";
            using (TextWriter txtWriter = new StreamWriter(filename, false, Encoding.ASCII))
            {
                txtWriter.WriteLine("SessionId,Activity,EventId,Source,SourceElapsed,Stage,Thread");

                int count = 0;
                foreach (Guid tracingId in PerformanceInstrumentation.TracingIds)
                {
                    Console.WriteLine("Query tracing info {0}", tracingId);
                    var tracingSession = protocol.QueryTracingInfo(tracingId);

                    StringBuilder sb = new StringBuilder();
                    foreach (TracingEvent te in tracingSession.TracingEvents)
                    {
                        sb.AppendFormat("{0},\"{1}\",{2},{3},{4},{5},{6}", tracingSession.SessionId, te.Activity, te.EventId, te.Source,
                                        te.SourceElapsed,
                                        te.Stage, te.Thread);
                        sb.AppendLine();
                    }

                    txtWriter.Write(sb);
                    ++count;
                }
            }
        }

        [Test]
        public void BinaryProtocolRunWritePerformanceSingleThread()
        {
            RunWritePerformanceSingleThread<BinaryProtocolWrapper>();
        }

        [Test]
        public void ThriftRunWritePerformanceSingleThread()
        {
            RunWritePerformanceSingleThread<ThriftProtocolWrapper>();
        }
    }
}