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

using System.Collections.Generic;
using System.Net;
using System.Numerics;
using CassandraSharp.Extensibility;
using CassandraSharp.Utils;

namespace CassandraSharp.EndpointStrategy
{
    /// <summary>
    ///     Will pick a node by it's token to choose the coordinator node by the row key of the query.
    ///     Requires QueryHints.
    ///     Where QueryHint's are ommitted, will fallback to RoundRobin
    /// </summary>
    internal sealed class TokenRingEndpointStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _healthyEndpoints;

        private readonly object _lock = new object();

        private readonly TokenRing _ring;

        private int _nextCandidate;

        public TokenRingEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = new List<IPAddress>(endpoints);
            _nextCandidate = 0;
            _ring = new TokenRing();
        }

        public void Ban(IPAddress endpoint)
        {
            lock (_lock)
            {
                if (_healthyEndpoints.Remove(endpoint))
                {
                    _ring.BanNode(endpoint);
                }
            }
        }

        public void Permit(IPAddress endpoint)
        {
            lock (_lock)
            {
                _healthyEndpoints.Add(endpoint);
                _ring.PermitNode(endpoint);
            }
        }

        public IPAddress Pick(BigInteger? token)
        {
            IPAddress endpoint = null;
            if (0 < _healthyEndpoints.Count)
            {
                if (token.HasValue && 0 < _ring.RingSize())
                {
                    //Attempt to binary search for key in token ring
                    lock (_lock)
                    {
                        endpoint = _ring.FindReplica(token.Value);
                    }
                }
                else
                {
                    //fallback to round robin when no hint supplied
                    lock (_lock)
                    {
                        _nextCandidate = (_nextCandidate + 1) % _healthyEndpoints.Count;
                        endpoint = _healthyEndpoints[_nextCandidate];
                    }
                }
            }

            return endpoint;
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            lock (_lock)
            {
                IPAddress endpoint = peer.RpcAddress;
                switch (kind)
                {
                    case NotificationKind.Add:
                        if (!_healthyEndpoints.Contains(endpoint))
                        {
                            _healthyEndpoints.Add(endpoint);
                            _ring.AddOrUpdateNode(peer);
                        }
                        break;

                    case NotificationKind.Update:
                        _ring.AddOrUpdateNode(peer);
                        break;

                    case NotificationKind.Remove:
                        if (_healthyEndpoints.Contains(endpoint))
                        {
                            _healthyEndpoints.Remove(endpoint);
                            _ring.RemoveNode(endpoint);
                        }
                        break;
                }
            }
        }
    }
}