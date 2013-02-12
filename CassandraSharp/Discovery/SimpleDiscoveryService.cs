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

namespace CassandraSharp.Discovery
{
    using System.Collections.Generic;
    using System.Net;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Extensibility;

    internal class SimpleDiscoveryService : IDiscoveryService
    {
        private readonly ILogger _logger;

        public SimpleDiscoveryService(ILogger logger)
        {
            _logger = logger;
        }

        public IEnumerable<IPAddress> DiscoverPeers(ICluster cluster)
        {
            ICqlCommand cqlCommand = new PocoCommand(cluster);
            var futPeers = cqlCommand.Execute<Peer>("select rpc_address from system.peers", ConsistencyLevel.ONE).AsFuture();
            List<IPAddress> newPeers = new List<IPAddress>();
            foreach (Peer peer in futPeers.Result)
            {
                IPAddress newPeer = peer.RpcAddress;
                newPeers.Add(newPeer);
                _logger.Debug("Discovered peer {0}", newPeer);
            }
            return newPeers;
        }

        internal class Peer
        {
            public IPAddress RpcAddress;
        }
    }
}