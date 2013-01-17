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
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Runtime.CompilerServices;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Snitch;
    using CassandraSharp.Utils;

    public class ClusterManager
    {
        private static CassandraSharpConfig _config;

        private static IRecoveryService _recoveryService;

        private static ILogger _logger;

        private static IInstrumentation _instrumentation;

        public static ICluster GetCluster(string name)
        {
            name.CheckArgumentNotNull("name");

            if (null == _config)
            {
                throw new InvalidOperationException("ClusterManager is not initialized");
            }

            ClusterConfig clusterConfig = GetClusterConfig(name);
            return GetCluster(clusterConfig);
        }

        public static ICluster GetCluster(ClusterConfig clusterConfig)
        {
            clusterConfig.CheckArgumentNotNull("clusterConfig");
            clusterConfig.Endpoints.CheckArgumentNotNull("clusterConfig.Endpoints");

            TransportConfig transportConfig = clusterConfig.Transport ?? new TransportConfig();
            IRecoveryService recoveryService = FindRecoveryService(transportConfig.Recoverable);

            // create endpoints
            IEndpointSnitch snitch = Factory.Create(clusterConfig.Endpoints.Snitch, _logger);
            IEnumerable<IPAddress> endpoints = clusterConfig.Endpoints.Servers.Select(NetworkFinder.Find);

            // create required services
            IEndpointStrategy endpointsManager = EndpointStrategy.Factory.Create(clusterConfig.Endpoints.Strategy, endpoints, snitch, _logger);
            IConnectionFactory connectionFactory = Transport.Factory.Create(transportConfig.Type, transportConfig, _logger);

            // create the cluster now
            ICluster cluster = Cluster.Factory.Create("Default", endpointsManager, _logger, connectionFactory, recoveryService, _instrumentation);

            IDiscoveryService discoveryService = Discovery.Factory.Create(clusterConfig.Endpoints.Discovery, _logger);
            var newPeers = discoveryService.DiscoverPeers(cluster);
            endpointsManager.Update(newPeers);

            return cluster;
        }

        private static ClusterConfig GetClusterConfig(string name)
        {
            ClusterConfig clusterConfig = (from config in _config.Clusters
                                           where config.Name == name
                                           select config).FirstOrDefault();
            if (null == clusterConfig)
            {
                string msg = string.Format("Can't find cluster configuration '{0}'", name);
                throw new KeyNotFoundException(msg);
            }

            return clusterConfig;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static IRecoveryService FindRecoveryService(bool recover)
        {
            return ! recover
                           ? null
                           : _recoveryService;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Shutdown()
        {
            if (null != _recoveryService)
            {
                _recoveryService.SafeDispose();
                _recoveryService = null;
            }

            _config = null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Configure(CassandraSharpConfig config)
        {
            config.CheckArgumentNotNull("config");

            if (null != _config)
            {
                throw new InvalidOperationException("ClusterManager is already initialized");
            }

            _logger = Logger.Factory.Create(config.Logger);
            _recoveryService = Recovery.Factory.Create(config.Recovery, _logger);
            _instrumentation = Instrumentation.Factory.Create(config.Instrumentation, _logger);
            _config = config;
        }
    }
}