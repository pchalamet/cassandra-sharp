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
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Extensibility;

    internal static class TracingHelpers
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

        private static void QueryAndPushTracingSessionWorker(IConnection connection, Guid tracingId, InstrumentationToken token,
                                                             IInstrumentation instrumentation,
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
            IDataMapperFactory facEvents = new DataMapperFactory<TracingEvent>(null);
            var obsEvents =
                    new CqlQuery<TracingEvent>(connection, queryEvents, facEvents)
                            .WithConsistencyLevel(ConsistencyLevel.ONE)
                            .WithExecutionFlags(ExecutionFlags.None);
            var tracingEvents = obsEvents.AsFuture().Result.ToList();
            tracingEvents.Sort(CompareTracingEvent);
            TracingEvent[] events = tracingEvents.ToArray();

            string querySession = "select * from system_traces.sessions where session_id=" + tracingId;

            IDataMapperFactory facSession = new DataMapperFactory<TracingSession>(null);
            var obsSession =
                    new CqlQuery<TracingSession>(connection, querySession, facSession)
                            .WithConsistencyLevel(ConsistencyLevel.ONE)
                            .WithExecutionFlags(ExecutionFlags.None);
            TracingSession tracingSession = obsSession.AsFuture().Result.Single();
            tracingSession.TracingEvents = events;

            instrumentation.ServerTrace(token, tracingSession);
        }
    }
}