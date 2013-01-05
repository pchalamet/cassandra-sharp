// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Runtime.CompilerServices;
    using CassandraSharp.Extensibility;

    internal class RandomEndpointStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _bannedEndpoints;

        private readonly List<IPAddress> _healthyEndpoints;

        private readonly Random _rnd;

        public RandomEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = new List<IPAddress>(endpoints);
            _bannedEndpoints = new List<IPAddress>();
            _rnd = new Random();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IPAddress Pick(Token token)
        {
            if (0 < _healthyEndpoints.Count)
            {
                int candidate = _rnd.Next(_healthyEndpoints.Count);
                IPAddress endpoint = _healthyEndpoints[candidate];
                return endpoint;
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Ban(IPAddress endpoint)
        {
            if (_healthyEndpoints.Remove(endpoint))
            {
                _bannedEndpoints.Add(endpoint);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Permit(IPAddress endpoint)
        {
            if (_bannedEndpoints.Remove(endpoint))
            {
                _healthyEndpoints.Add(endpoint);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Update(IEnumerable<IPAddress> endpoints)
        {
            foreach (IPAddress endpoint in endpoints)
            {
                if (!_healthyEndpoints.Contains(endpoint) && !_bannedEndpoints.Contains(endpoint))
                {
                    _healthyEndpoints.Add(endpoint);
                }
            }
        }
    }
}