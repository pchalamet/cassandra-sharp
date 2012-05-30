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
    using System.Collections.Generic;
    using System.Net;
    using System.Runtime.CompilerServices;
    using CassandraSharp.Utils;

    internal class NearestEndpointStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _bannedEndpoints;

        private readonly List<IPAddress> _healthyEndpoints;

        public NearestEndpointStrategy(IEnumerable<IPAddress> endpoints, IEndpointSnitch snitch)
        {
            IPAddress clientAddress = NetworkFinder.Find(Dns.GetHostName());
            _healthyEndpoints = snitch.GetSortedListByProximity(clientAddress, endpoints);
            _bannedEndpoints = new List<IPAddress>();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IPAddress Pick(byte[] keyHint)
        {
            if (0 < _healthyEndpoints.Count)
            {
                return _healthyEndpoints[0];
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Ban(IPAddress endPoint)
        {
            if (_healthyEndpoints.Remove(endPoint))
            {
                _bannedEndpoints.Add(endPoint);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Permit(IPAddress endPoint)
        {
            if (_bannedEndpoints.Remove(endPoint))
            {
                _healthyEndpoints.Add(endPoint);
            }
        }
    }
}