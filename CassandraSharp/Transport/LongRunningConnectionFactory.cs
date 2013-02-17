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

namespace CassandraSharp.Transport
{
    using System.Net;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;

    internal class LongRunningConnectionFactory : IConnectionFactory
    {
        private readonly TransportConfig _config;

        private readonly IInstrumentation _instrumentation;

        private readonly ILogger _logger;

        public LongRunningConnectionFactory(TransportConfig config, ILogger logger, IInstrumentation instrumentation)
        {
            _config = config;
            _logger = logger;
            _instrumentation = instrumentation;
        }

        public IConnection Create(IPAddress address)
        {
            _logger.Debug("Creating connection to {0}", address);

            LongRunningConnection connection = new LongRunningConnection(address, _config, _logger, _instrumentation);
            return connection;
        }
    }
}