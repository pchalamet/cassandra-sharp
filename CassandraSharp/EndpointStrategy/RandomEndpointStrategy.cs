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

using System;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using CassandraSharp.Extensibility;

namespace CassandraSharp.EndpointStrategy
{
    internal sealed class RandomEndpointStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _bannedEndpoints;

        private readonly List<IPAddress> _healthyEndpoints;

        private readonly object _lock = new object();

        private readonly Random _rnd;

        public RandomEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = new List<IPAddress>(endpoints);
            _bannedEndpoints = new List<IPAddress>();
            _rnd = new Random();
        }

        public IPAddress Pick(BigInteger? token)
        {
            lock (_lock)
            {
                IPAddress endpoint = null;
                if (0 < _healthyEndpoints.Count)
                {
                    var candidate = _rnd.Next(_healthyEndpoints.Count);
                    endpoint = _healthyEndpoints[candidate];
                }

                return endpoint;
            }
        }

        public void Ban(IPAddress endpoint)
        {
            lock (_lock)
            {
                if (_healthyEndpoints.Remove(endpoint)) _bannedEndpoints.Add(endpoint);
            }
        }

        public void Permit(IPAddress endpoint)
        {
            lock (_lock)
            {
                if (_bannedEndpoints.Remove(endpoint)) _healthyEndpoints.Add(endpoint);
            }
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            lock (_lock)
            {
                var endpoint = peer.RpcAddress;
                if (!_healthyEndpoints.Contains(endpoint) && !_bannedEndpoints.Contains(endpoint)) _healthyEndpoints.Add(endpoint);
            }
        }
    }
}