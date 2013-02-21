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

namespace CassandraSharp.Discovery
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Numerics;
    using System.Reactive.Linq;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Extensibility;

    internal class SystemPeersDiscoveryService : IDiscoveryService
    {
        private readonly ILogger _logger;

        public SystemPeersDiscoveryService(ILogger logger)
        {
            _logger = logger;
        }

        public IEnumerable<Peer> DiscoverPeers(ICluster cluster)
        {
            try
            {
                IDataMapper mapper = new PocoCommand.PocoDataMapperFactory();
                IConnection connection = cluster.GetConnection();

                IDataMapperFactory peerFactory = mapper.Create<DiscoveredPeer>();
                var obsLocalPeer = CQLCommandHelpers.CreateQuery(connection, "select tokens from system.local",
                                                                 ConsistencyLevel.ONE, peerFactory, ExecutionFlags.None).Cast<DiscoveredPeer>();
                var taskLocalPeer = obsLocalPeer.AsFuture();

                var obsPeers = CQLCommandHelpers.CreateQuery(connection, "select rpc_address,tokens from system.peers",
                                                             ConsistencyLevel.ONE, peerFactory, ExecutionFlags.None).Cast<DiscoveredPeer>();
                var taskPeers = obsPeers.AsFuture();

                taskPeers.Wait();
                taskLocalPeer.Wait();

                List<Peer> result = new List<Peer>();
                foreach (var op in taskPeers.Result)
                {
                    var peer = new Peer
                        {
                                RpcAddress = op.RpcAddress,
                                Tokens = op.Tokens.Select(BigInteger.Parse).ToArray()
                        };
                    result.Add(peer);
                }

                var localPeer = (from lp in taskLocalPeer.Result
                                 select new Peer {RpcAddress = connection.Endpoint, Tokens = lp.Tokens.Select(BigInteger.Parse).ToArray()}).Single();
                result.Add(localPeer);

                return result;
            }
            catch (Exception ex)
            {
                _logger.Error("Discovery failed with error {0}", ex);
            }

            return Enumerable.Empty<Peer>();
        }

        private class DiscoveredPeer
        {
            public IPAddress RpcAddress { get; internal set; }

            public ISet<string> Tokens { get; internal set; }
        }
    }
}