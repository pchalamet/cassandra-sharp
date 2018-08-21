// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Net;
    using CassandraSharp.Extensibility;

    public class PerformanceInstrumentation : IInstrumentation
    {
        private static readonly object _lock = new object();

        private static readonly List<Guid> _tracingIds = new List<Guid>();

        private static Stopwatch _readWatch = new Stopwatch();

        private static Stopwatch _writeWatch = new Stopwatch();

        public static long TotalRead
        {
            get { return _readWatch.ElapsedMilliseconds; }
        }

        public static long TotalWrite
        {
            get { return _writeWatch.ElapsedMilliseconds; }
        }

        public static List<Guid> TracingIds
        {
            get { return _tracingIds; }
        }

        public void ClientQuery(InstrumentationToken token)
        {
        }

        public void ClientConnectionInfo(InstrumentationToken token, IPAddress coordinator, ushort streamId)
        {
        }

        public void ClientTrace(InstrumentationToken token, EventType eventType)
        {
            switch (eventType)
            {
                case EventType.BeginRead:
                    _readWatch.Start();
                    break;

                case EventType.EndRead:
                    _readWatch.Stop();
                    break;

                case EventType.BeginWrite:
                    _writeWatch.Start();
                    break;

                case EventType.EndWrite:
                    _writeWatch.Stop();
                    break;
            }
        }

        public void ServerTrace(InstrumentationToken token, Guid traceId)
        {
            lock (_lock)
            {
                _tracingIds.Add(traceId);
            }
        }

        public void Dispose()
        {
        }

        public static void Initialize()
        {
            _tracingIds.Clear();
            _writeWatch = new Stopwatch();
            _readWatch = new Stopwatch();
        }
    }
}