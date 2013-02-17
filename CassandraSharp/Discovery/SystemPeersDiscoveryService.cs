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

namespace CassandraSharp.Discovery
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Reactive.Linq;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Extensibility;

    internal class SystemPeersDiscoveryService : IDiscoveryService
    {
        private readonly ILogger _logger;

        public SystemPeersDiscoveryService(ILogger logger)
        {
            _logger = logger;
        }

        public IEnumerable<IPAddress> DiscoverPeers(ICluster cluster)
        {
            try
            {
                ICqlCommand cqlCommand = new PocoCommand(cluster);
                IObservable<Peer> obsPeers = cqlCommand.Execute<Peer>("select rpc_address from system.peers", ConsistencyLevel.ONE);
                var taskPeers = obsPeers.Select(x => x.RpcAddress).AsFuture();
                taskPeers.Wait();
                return taskPeers.Result;
            }
            catch (Exception ex)
            {
                _logger.Error("Discovery failed with error {0}", ex);
            }

            return Enumerable.Empty<IPAddress>();
        }

        internal class Peer
        {
            public IPAddress RpcAddress;
        }
    }
}