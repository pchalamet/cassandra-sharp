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

namespace CassandraSharp.EndpointStrategy
{
    using System.Collections.Generic;
    using System.Net;
    using System.Numerics;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal class NearestEndpointStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _bannedEndpoints;

        private readonly IPAddress _clientAddress;

        private readonly List<IPAddress> _healthyEndpoints;

        private readonly object _lock = new object();

        private readonly IEndpointSnitch _snitch;

        public NearestEndpointStrategy(IEnumerable<IPAddress> endpoints, IEndpointSnitch snitch)
        {
            _snitch = snitch;
            IPAddress clientAddress = NetworkFinder.Find(Dns.GetHostName());
            _healthyEndpoints = snitch.GetSortedListByProximity(clientAddress, endpoints);
            _bannedEndpoints = new List<IPAddress>();
            _clientAddress = NetworkFinder.Find(Dns.GetHostName());
        }

        public IPAddress Pick(BigInteger? token = null)
        {
            lock (_lock)
            {
                IPAddress endpoint = null;
                if (0 < _healthyEndpoints.Count)
                {
                    endpoint = _healthyEndpoints[0];
                }

                return endpoint;
            }
        }

        public void Ban(IPAddress endPoint)
        {
            lock (_lock)
            {
                if (_healthyEndpoints.Remove(endPoint))
                {
                    _bannedEndpoints.Add(endPoint);
                }
            }
        }

        public void Permit(IPAddress endPoint)
        {
            lock (_lock)
            {
                if (_bannedEndpoints.Remove(endPoint))
                {
                    _healthyEndpoints.Add(endPoint);
                    _healthyEndpoints.Sort((a1, a2) => _snitch.CompareEndpoints(_clientAddress, a1, a2));
                }
            }
        }

        public void Update(IEnumerable<Peer> endpoints)
        {
            lock (_lock)
            {
                bool updated = false;
                foreach (Peer peer in endpoints)
                {
                    IPAddress endpoint = peer.RpcAddress;
                    if (!_healthyEndpoints.Contains(endpoint) && !_bannedEndpoints.Contains(endpoint))
                    {
                        _healthyEndpoints.Add(endpoint);
                        updated = true;
                    }
                }

                if (updated)
                {
                    _healthyEndpoints.Sort((a1, a2) => _snitch.CompareEndpoints(_clientAddress, a1, a2));
                }
            }
        }
    }
}