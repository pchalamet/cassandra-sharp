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

    /// <summary>
    ///     Will loop through nodes to perfectly evenly spread load.
    /// </summary>
    internal sealed class RoundRobinEndpointStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _bannedEndpoints;

        private readonly List<IPAddress> _healthyEndpoints;

        private readonly object _lock = new object();

        private int _nextCandidate;

        public RoundRobinEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = new List<IPAddress>(endpoints);
            _bannedEndpoints = new List<IPAddress>();
            _nextCandidate = 0;
        }

        public void Ban(IPAddress endpoint)
        {
            lock (_lock)
            {
                if (_healthyEndpoints.Remove(endpoint))
                {
                    _bannedEndpoints.Add(endpoint);
                }
            }
        }

        public void Permit(IPAddress endpoint)
        {
            lock (_lock)
            {
                if (_bannedEndpoints.Remove(endpoint))
                {
                    _healthyEndpoints.Add(endpoint);
                }
            }
        }

        public IPAddress Pick(BigInteger? token)
        {
            lock (_lock)
            {
                IPAddress endpoint = null;
                if (0 < _healthyEndpoints.Count)
                {
                    _nextCandidate = (_nextCandidate + 1) % _healthyEndpoints.Count;
                    endpoint = _healthyEndpoints[_nextCandidate];
                }

                return endpoint;
            }
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            lock (_lock)
            {
                IPAddress endpoint = peer.RpcAddress;
                switch (kind)
                {
                    case NotificationKind.Add:
                    case NotificationKind.Update:
                        if (!_healthyEndpoints.Contains(endpoint) && !_bannedEndpoints.Contains(endpoint))
                        {
                            _healthyEndpoints.Add(endpoint);
                        }
                        break;
                    case NotificationKind.Remove:
                        if (_healthyEndpoints.Contains(endpoint))
                        {
                            _healthyEndpoints.Remove(endpoint);
                        }
                        break;
                }
            }
        }
    }
}