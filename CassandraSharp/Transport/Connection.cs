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

namespace CassandraSharp.Transport
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;

    internal class Connection : IConnection,
                                IDisposable
    {
        private readonly TransportConfig _config;

        private readonly IInstrumentation _instrumentation;

        private readonly ILogger _logger;

        public Connection(IPAddress address, TransportConfig config, ILogger logger, IInstrumentation instrumentation)
        {
            _config = config;
            _logger = logger;
            _instrumentation = instrumentation;
            Endpoint = address;
        }

        public IPAddress Endpoint { get; private set; }

        public event EventHandler<FailureEventArgs> OnFailure;

        public IQuery Execute(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader, ExecutionFlags executionFlags,
                              InstrumentationToken token, Action<IEnumerable<object>> dataAvailable)
        {
            return new Query(writer, reader, executionFlags, token, dataAvailable, this);
        }

        public void Dispose()
        {
        }
    }
}