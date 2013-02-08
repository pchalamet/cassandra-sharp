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

namespace CassandraSharp.Instrumentation
{
    using System;
    using System.Net;

    public class TracingEvent
    {
        public TracingEvent(string activity, Guid eventId, IPAddress source, long sourceElapsed, string thread)
        {
            Activity = activity;
            EventId = eventId;
            Source = source;
            SourceElapsed = sourceElapsed;
            Thread = thread;
        }

        public string Activity { get; private set; }

        public Guid EventId { get; private set; }

        public IPAddress Source { get; private set; }

        public long SourceElapsed { get; private set; }

        public string Thread { get; private set; }
    }
}