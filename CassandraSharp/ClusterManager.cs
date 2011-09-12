// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
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
    using CassandraSharp.Snitch;
    using CassandraSharp.Transport;
    using CassandraSharp.Utils;

    public class ClusterManager
    {
        private static CassandraSharpConfig _config;

        [MethodImpl(MethodImplOptions.Synchronized)]
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

            // create endpoints
            ISnitch snitch = clusterConfig.Endpoints.Snitch.Create();
            IPAddress clientAddress = NetworkFinder.Find(Dns.GetHostName());
            IEnumerable<Endpoint> endpoints = GetEndpoints(clusterConfig.Endpoints, snitch, clientAddress);

            // create endpoint strategy
            IEndpointStrategy endpointsManager = clusterConfig.Endpoints.Create(endpoints);
            IPool<IConnection> pool = PoolType.Stack.Create(clusterConfig.Behavior.PoolSize);

            // create the cluster now
            ITransportFactory transportFactory = clusterConfig.Transport.Create();
            return new Cluster(clusterConfig.Behavior, pool, transportFactory, endpointsManager);
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
                    int proximity = snitch.ComputeProximity(clientAddress, serverAddress);
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
        public static void Shutdown()
        {
            if (null == _config)
            {
                return;
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