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

namespace CassandraSharp.Instrumentation
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.CQLPropertyBag;
    using CassandraSharp.Extensibility;

    internal static class TracingExtensions
    {
        private static int CompareTracingEvent(TracingEvent x, TracingEvent y)
        {
            if (x.SourceElapsed < y.SourceElapsed)
            {
                return -1;
            }

            if (x.SourceElapsed > y.SourceElapsed)
            {
                return 1;
            }

            return 0;
        }

        public static void AsyncQueryAndPushTracingSession(IConnection connection, Guid tracingId, InstrumentationToken token, IInstrumentation instrumentation,
                                                           ILogger logger)
        {
            Task.Factory.StartNew(() => QueryAndPushTracingSessionWorker(connection, tracingId, token, instrumentation, logger));
        }

        private static void QueryAndPushTracingSessionWorker(IConnection connection, Guid tracingId, InstrumentationToken token, IInstrumentation instrumentation,
                                                            ILogger logger)
        {
            try
            {
                QueryAndPushTracingSession(connection, tracingId, token, instrumentation);
            }
            catch (Exception ex)
            {
                logger.Error("Failed to read performance for {0}: {1}", tracingId, ex);
            }
        }

        private static void QueryAndPushTracingSession(IConnection connection, Guid tracingId, InstrumentationToken token, IInstrumentation instrumentation)
        {
            string queryEvents = "select * from system_traces.events where session_id=" + tracingId;
            List<TracingEvent> tracingEvents = new List<TracingEvent>();
            IDataMapperFactory factory = new DataMapperFactory(null);
            foreach (
                    IDictionary<string, object> mapEvents in
                            CQLCommandHelpers.CreateQuery(connection, queryEvents, ConsistencyLevel.ONE, factory, ExecutionFlags.None).AsFuture().Result)
            {
                TracingEvent tracingEvent = new TracingEvent((string) mapEvents["activity"],
                                                             (Guid) mapEvents["event_id"],
                                                             (IPAddress) mapEvents["source"],
                                                             (int) mapEvents["source_elapsed"],
                                                             (string) mapEvents["thread"]);
                tracingEvents.Add(tracingEvent);
            }
            tracingEvents.Sort(CompareTracingEvent);
            TracingEvent[] events = tracingEvents.ToArray();

            string querySession = "select * from system_traces.sessions where session_id=" + tracingId.ToString();
            IDictionary<string, object> mapSession =
                    (IDictionary<string, object>)
                    CQLCommandHelpers.CreateQuery(connection, querySession, ConsistencyLevel.ONE, factory, ExecutionFlags.None).AsFuture().Result.Single();
            TracingSession tracingSession = new TracingSession((IPAddress) mapSession["coordinator"],
                                                               (int) mapSession["duration"],
                                                               (IDictionary<string, string>) mapSession["parameters"],
                                                               (string) mapSession["request"],
                                                               (Guid) mapSession["session_id"],
                                                               (DateTime) mapSession["started_at"],
                                                               events);

            instrumentation.ServerTrace(token, tracingSession);
        }
    }
}