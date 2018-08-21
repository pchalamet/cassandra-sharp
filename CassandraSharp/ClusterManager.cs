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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using CassandraSharp.Config;
using CassandraSharp.Extensibility;
using CassandraSharp.Utils;

namespace CassandraSharp
{
    public sealed class ClusterManager : IClusterManager
    {
        private readonly object _lock = new object();

        private CassandraSharpConfig _config;

        private IInstrumentation _instrumentation;

        private ILogger _logger;

        private IRecoveryService _recoveryService;

        public ClusterManager(CassandraSharpConfig config)
        {
            lock (_lock)
            {
                config.CheckArgumentNotNull("config");
                _logger = ServiceActivator<Logger.Factory>.Create<ILogger>(config.Logger.Type, config.Logger);
                _recoveryService = ServiceActivator<Recovery.Factory>.Create<IRecoveryService>(config.Recovery.Type, config.Recovery, _logger);
                _instrumentation = ServiceActivator<Instrumentation.Factory>.Create<IInstrumentation>(config.Instrumentation.Type, config.Instrumentation);
                _config = config;
            }
        }

        public ICluster GetCluster(string name)
        {
            name.CheckArgumentNotNull("name");

            if (null == _config) throw new InvalidOperationException("ClusterManager is not initialized");

            var clusterConfig = GetClusterConfig(name);
            return GetCluster(clusterConfig);
        }

        public ICluster GetCluster(ClusterConfig clusterConfig)
        {
            clusterConfig.CheckArgumentNotNull("clusterConfig");
            clusterConfig.Endpoints.CheckArgumentNotNull("clusterConfig.Endpoints");

            var transportConfig = clusterConfig.Transport ?? new TransportConfig();
            var recoveryService = GetRecoveryService(transportConfig.Recoverable);
            var keyspaceConfig = clusterConfig.DefaultKeyspace ?? new KeyspaceConfig();

            // create endpoints
            var snitch = ServiceActivator<Snitch.Factory>.Create<IEndpointSnitch>(clusterConfig.Endpoints.Snitch, _logger);
            IEnumerable<IPAddress> endpoints = clusterConfig.Endpoints.Servers.Select(Network.Find).Where(x => null != x).ToArray();
            if (!endpoints.Any()) throw new ArgumentException("Expecting at least one valid endpoint");

            // create required services
            var endpointsManager = ServiceActivator<EndpointStrategy.Factory>.Create<IEndpointStrategy>(clusterConfig.Endpoints.Strategy,
                                                                                                        endpoints, snitch,
                                                                                                        _logger, clusterConfig.Endpoints);
            var connectionFactory = ServiceActivator<Transport.Factory>.Create<IConnectionFactory>(transportConfig.Type, transportConfig, keyspaceConfig,
                                                                                                   _logger,
                                                                                                   _instrumentation);

            var partitioner = ServiceActivator<Partitioner.Factory>.Create<IPartitioner>(clusterConfig.Partitioner);

            // create the cluster now
            var cluster = ServiceActivator<Cluster.Factory>.Create<ICluster>(clusterConfig.Type, endpointsManager, _logger, connectionFactory,
                                                                             recoveryService, partitioner, clusterConfig);

            var discoveryService = ServiceActivator<Discovery.Factory>.Create<IDiscoveryService>(clusterConfig.Endpoints.Discovery.Type,
                                                                                                 clusterConfig.Endpoints.Discovery,
                                                                                                 _logger,
                                                                                                 cluster);
            discoveryService.OnTopologyUpdate += endpointsManager.Update;
            cluster.OnClosed += discoveryService.SafeDispose;

            return cluster;
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _recoveryService.SafeDispose();
                _recoveryService = null;

                _instrumentation.SafeDispose();
                _instrumentation = null;

                _logger.SafeDispose();
                _logger = null;

                _config = null;
            }
        }

        private ClusterConfig GetClusterConfig(string name)
        {
            var clusterConfig = (from config in _config.Clusters
                                 where config.Name == name
                                 select config).FirstOrDefault();
            if (null == clusterConfig)
            {
                var msg = string.Format("Can't find cluster configuration '{0}'", name);
                throw new KeyNotFoundException(msg);
            }

            return clusterConfig;
        }

        private IRecoveryService GetRecoveryService(bool recover)
        {
            lock (_lock)
            {
                return !recover
                           ? null
                           : _recoveryService;
            }
        }
    }
}