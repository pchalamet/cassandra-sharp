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
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Numerics;
    using CassandraSharp.Extensibility;

    internal class TokenAwareStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _endpoints = new List<IPAddress>();

        private readonly object _lock = new object();

        private readonly SortedDictionary<BigInteger, int> _ring = new SortedDictionary<BigInteger, int>();

        private readonly List<bool> _stateEndpoints = new List<bool>();

        public TokenAwareStrategy(IEnumerable<IPAddress> endpoints)
        {
            foreach (IPAddress endpoint in endpoints)
            {
                Enable(endpoint, true);
            }
        }

        public IPAddress Pick(BigInteger? token = null)
        {
            if (token.HasValue)
            {
                throw new ArgumentException();
            }

            BigInteger tokenValue = token.Value;
            lock (_lock)
            {
                int idx = _endpoints.Count - 1;
                foreach (KeyValuePair<BigInteger, int> kvp in _ring)
                {
                    if (tokenValue < kvp.Key)
                    {
                        idx = kvp.Value;
                        break;
                    }
                }

                return _endpoints[idx];
            }
        }

        public void Ban(IPAddress endpoint)
        {
            lock (_lock)
                Enable(endpoint, false);
        }

        public void Permit(IPAddress endpoint)
        {
            lock (_lock)
                Enable(endpoint, true);
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            lock (_lock)
            {
                int idx = _endpoints.IndexOf(peer.RpcAddress);
                if (-1 == idx)
                {
                    idx = Enable(peer.RpcAddress, true);
                }

                switch (kind)
                {
                    case NotificationKind.Add:
                    case NotificationKind.Update:
                        foreach (BigInteger token in peer.Tokens)
                        {
                            _ring[token] = idx;
                        }
                        break;

                    case NotificationKind.Remove:
                        _stateEndpoints.RemoveAt(idx);
                        _endpoints.RemoveAt(idx);
                        foreach (BigInteger token in peer.Tokens)
                        {
                            _ring.Remove(token);
                        }
                        break;
                }
            }
        }

        private int Enable(IPAddress endpoint, bool state)
        {
            int idx = _endpoints.IndexOf(endpoint);
            if (-1 != idx)
            {
                _stateEndpoints[idx] = state;
            }

            return idx;
        }
    }
}