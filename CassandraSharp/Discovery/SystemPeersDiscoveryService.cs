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
    using System.Timers;
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.Config;
    using CassandraSharp.Enlightenment;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal sealed class SystemPeersDiscoveryService : IDiscoveryService
    {
        private readonly ICluster _cluster;

        private readonly ILogger _logger;

        private readonly IDataMapper _peerFactory;

        private readonly Timer _timer;

        public SystemPeersDiscoveryService(ILogger logger, ICluster cluster, DiscoveryConfig config)
        {
            IDataMapperFactory mapper = new PocoDataMapperFactory();
            _peerFactory = mapper.Create<DiscoveredPeer>();

            _logger = logger;
            _cluster = cluster;
            _timer = new Timer(config.Interval * 1000);
            _timer.Elapsed += (s, e) => TryDiscover();
            _timer.AutoReset = true;

            TryDiscover();
        }

        public void Dispose()
        {
            _timer.SafeDispose();
        }

        public event TopologyUpdate OnTopologyUpdate;

        private void Notify(IPAddress rpcAddress, string datacenter, string rack, IEnumerable<string> tokens)
        {
            if (!Network.IsValidEndpoint(rpcAddress))
            {
                _logger.Warn("Discovered invalid endpoint {0}", rpcAddress);
                return;
            }

            if (null != OnTopologyUpdate)
            {
                Peer peer = new Peer
                    {
                            RpcAddress = rpcAddress,
                            Datacenter = datacenter,
                            Rack = rack,
                            Tokens = tokens.Select(BigInteger.Parse).ToArray()
                    };

                _logger.Info("Discovered peer {0}, {1}, {2}", rpcAddress, datacenter, rack);

                OnTopologyUpdate(NotificationKind.Update, peer);
            }
        }

        private void TryDiscover()
        {
            try
            {
                IConnection connection = _cluster.GetConnection();

                var obsLocalPeer = new CqlQuery<DiscoveredPeer>(connection, ConsistencyLevel.ONE, ExecutionFlags.None, "select data_center,rack,tokens from system.local",
                                                                _peerFactory);
                obsLocalPeer.Subscribe(x => Notify(connection.Endpoint, x.Datacenter, x.Rack, x.Tokens), ex => _logger.Error("SystemPeersDiscoveryService failed with error {0}", ex));

                var obsPeers =
                        new CqlQuery<DiscoveredPeer>(connection, ConsistencyLevel.ONE, ExecutionFlags.None, "select rpc_address,data_center,rack,tokens from system.peers",
                                                     _peerFactory);
                obsPeers.Subscribe(x => Notify(x.RpcAddress, x.Datacenter, x.Rack, x.Tokens), ex => _logger.Error("SystemPeersDiscoveryService failed with error {0}", ex));
            }
            catch (Exception ex)
            {
                _logger.Error("SystemPeersDiscoveryService failed with error {0}", ex);
            }
        }

        internal class DiscoveredPeer
        {
            public IPAddress RpcAddress { get; internal set; }

            public HashSet<string> Tokens { get; internal set; }

            public string Rack { get; internal set; }

            public string Datacenter { get; internal set; }
        }
    }
}