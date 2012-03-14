namespace CassandraSharp
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Runtime.CompilerServices;
    using CassandraSharp.Config;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Factory;
    using CassandraSharp.Pool;
    using CassandraSharp.Recovery;
    using CassandraSharp.Snitch;
    using CassandraSharp.Transport;
    using CassandraSharp.Utils;

    public class ClusterManager
    {
        private static CassandraSharpConfig _config;

        private static IRecoveryService _recoveryService;

        public static ICluster GetCluster(string name)
        {
            if (null == _config)
            {
                throw new InvalidOperationException("ClusterManager is not initialized");
            }

            if (null == name)
            {
                throw new ArgumentNullException("name");
            }

            ClusterConfig clusterConfig = GetClusterConfig(name);
            return GetCluster(clusterConfig);
        }

        public static ICluster GetCluster(ClusterConfig clusterConfig)
        {
            if (null == clusterConfig)
            {
                throw new ArgumentNullException("clusterConfig");
            }

            if (null == clusterConfig.Endpoints)
            {
                throw new ArgumentNullException("clusterConfig.Endpoints");
            }

            IBehaviorConfig behaviorConfig = clusterConfig.BehaviorConfig ?? new BehaviorConfig();
            TransportConfig transportConfig = clusterConfig.Transport ?? new TransportConfig();

            IRecoveryService recoveryService = FindRecoveryService(transportConfig.Recoverable);

            // create endpoints
            ISnitch snitch;
            if (null == clusterConfig.Endpoints.SnitchType)
            {
                snitch = clusterConfig.Endpoints.Snitch.Create();
            }
            else
            {
                Type snitchType = Type.GetType(clusterConfig.Endpoints.SnitchType, true);
                snitch = (ISnitch)Activator.CreateInstance(snitchType);
            }

            IPAddress clientAddress = NetworkFinder.Find(Dns.GetHostName());
            IEnumerable<Endpoint> endpoints = GetEndpoints(clusterConfig.Endpoints, snitch, clientAddress);

            // create endpoint strategy
            IEndpointStrategy endpointsManager;
            if (null == clusterConfig.Endpoints.StrategyType)
            {
                endpointsManager = clusterConfig.Endpoints.Create(endpoints);
            }
            else
            {
                Type strategyType = Type.GetType(clusterConfig.Endpoints.StrategyType, true);
                endpointsManager = (IEndpointStrategy) Activator.CreateInstance(strategyType, endpoints);
            }
            IPool<IConnection> pool = PoolType.Stack.Create(transportConfig.PoolSize);

            // create the cluster now
            ITransportFactory transportFactory = transportConfig.Create();
            return new Cluster(behaviorConfig, pool, transportFactory, endpointsManager, recoveryService);
        }

        private static IEnumerable<Endpoint> GetEndpoints(EndpointsConfig config, ISnitch snitch, IPAddress clientAddress)
        {
            List<Endpoint> endpoints = new List<Endpoint>();
            foreach (string server in config.Servers)
            {
                IPAddress serverAddress = NetworkFinder.Find(server);
                if (null != serverAddress)
                {
                    string datacenter = snitch.GetDataCenter(serverAddress);
                    int proximity = snitch.ComputeDistance(clientAddress, serverAddress);
                    Endpoint endpoint = new Endpoint(server, serverAddress, datacenter, proximity);
                    endpoints.Add(endpoint);
                }
            }

            return endpoints;
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
            if (! recover)
            {
                return null;
            }

            if (null == _recoveryService)
            {
                _recoveryService = RecoveryServiceExtensions.Create();
            }

            return _recoveryService;
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
            if (null != _config)
            {
                throw new InvalidOperationException("ClusterManager is already initialized");
            }

            if (null == config)
            {
                throw new ArgumentNullException("config");
            }

            _config = config;
        }
    }
}