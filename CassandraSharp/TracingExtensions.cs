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

namespace CassandraSharp
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Extensibility;

    public static class TracingExtensions
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

        public static TracingSession QueryTracingInfo(this ICluster @this, Guid tracingId)
        {
            var cmd = @this.CreatePocoCommand();

            // query events and session
            string queryEvents = "select * from system_traces.events where session_id = " + tracingId;
            var obsEvents = cmd.WithConsistencyLevel(ConsistencyLevel.ONE)
                               .Execute<TracingEvent>(queryEvents)
                               .AsFuture();

            string querySession = "select * from system_traces.sessions where session_id = " + tracingId;
            var obsSession = cmd.WithConsistencyLevel(ConsistencyLevel.ONE)
                                .Execute<TracingSession>(querySession)
                                .AsFuture();

            Task.WaitAll(obsEvents, obsSession);

            // format the events
            var tracingEvents = obsEvents.Result.ToList();
            tracingEvents.Sort(CompareTracingEvent);
            TracingEvent[] events = tracingEvents.ToArray();
            foreach (var evt in events)
            {
                string[] tmp = evt.Thread.Split(':');
                evt.Stage = tmp[0];
                evt.Thread = tmp[1];
            }

            // build the result
            TracingSession tracingSession = obsSession.Result.Single();
            tracingSession.TracingEvents = events;
            return tracingSession;
        }
    }
}