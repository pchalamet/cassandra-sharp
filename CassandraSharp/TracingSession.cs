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

namespace CassandraSharp
{
    using System;
    using System.Net;

    public class TracingSession
    {
        public TracingSession(IPAddress coordinator, long duration, string parameters, string request, Guid sessionId, DateTime startedAt,
                              TracingEvent[] tracingEvents)
        {
            Coordinator = coordinator;
            Duration = duration;
            Parameters = parameters;
            Request = request;
            SessionId = sessionId;
            StartedAt = startedAt;
            TracingEvents = tracingEvents;
        }

        public IPAddress Coordinator { get; private set; }

        public long Duration { get; private set; }

        public string Parameters { get; private set; }

        public string Request { get; private set; }

        public Guid SessionId { get; private set; }

        public DateTime StartedAt { get; private set; }

        public TracingEvent[] TracingEvents { get; private set; }
    }
}