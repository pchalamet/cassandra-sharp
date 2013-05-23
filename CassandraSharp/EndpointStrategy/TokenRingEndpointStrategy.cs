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
    using System.Linq;
    using System.Net;
    using System.Numerics;
    using System.Text;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.Extensibility;

    /// <summary>
    /// Will pick a node by it's token to choose the coordinator node by the row key of the query.
    /// Requires QueryHints.
    /// Where QueryHint's are ommitted, will fallback to RoundRobin
    /// </summary>
    internal sealed class TokenRingEndpointStrategy : IEndpointStrategy
    {
        private readonly List<IPAddress> _healthyEndpoints;
        private readonly object _lock = new object();
        private int _roundRobinPointer;
        private readonly TokenRing _ring;



        public TokenRingEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = new List<IPAddress>(endpoints);
            _roundRobinPointer = 0;
            _ring = new TokenRing();
        }



        public void Ban(IPAddress endpoint)
        {
            lock (_lock)
            {
                if (_healthyEndpoints.Remove(endpoint))
                {
                    _ring.banNode(endpoint);
                }
            }
        }



        public void Permit(IPAddress endpoint)
        {
            lock (_lock)
            {
                _healthyEndpoints.Add(endpoint);
                _ring.permitNode(endpoint);
            }
        }



        public IPAddress Pick(QueryHint hint)
        {
            IPAddress endpoint = null;
            if (0 < _healthyEndpoints.Count)
            {
                if (hint != null && _ring.ringSize() > 0)
                {
                    //Attempt to binary search for key in token ring
                    lock (_lock)
                    {
                        endpoint = _ring.findReplica(hint.Key);
                    }
                }
                else
                {
                    //fallback to round robin when no hint supplied
                    lock (_lock)
                    {
                        endpoint = _healthyEndpoints[_roundRobinPointer++];
                        _roundRobinPointer %= _healthyEndpoints.Count;
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
                            _ring.addOrUpdateNode(peer);
                        }
                        break;
                    case NotificationKind.Update:
                        _ring.addOrUpdateNode(peer);
                        break;
                    case NotificationKind.Remove:
                        if (_healthyEndpoints.Contains(endpoint))
                        {
                            _healthyEndpoints.Remove(endpoint);
                            _ring.removeNode(endpoint);
                        }
                        break;
                }
            }
        }

        /// <summary>
        /// Represents spread of tokens around nodes
        /// </summary>
        internal sealed class TokenRing
        {
            private SortedDictionary<BigInteger, IPAddress> _permittedTokenRing;
            private SortedDictionary<BigInteger, IPAddress> _bannedTokenRing;
            private List<BigInteger> _tokenCache; //sorted list of tokens: for efficiency of binary search

            internal TokenRing()
            {
                _permittedTokenRing = new SortedDictionary<BigInteger, IPAddress>();
                _bannedTokenRing = new SortedDictionary<BigInteger, IPAddress>();
                _tokenCache = new List<BigInteger>();
            }

            /// <summary>
            /// Use binary search to find first token that is smaller than key
            /// </summary>
            /// <param name="key">Row key as BigInteger hash</param>
            /// <returns>IPAddress of node that owns the token</returns>
            internal IPAddress findReplica(BigInteger key)
            {
                int i = _tokenCache.BinarySearch(key);

                if (i < 0)
                {
                    i = ~i - 1;
                    if (i >= _permittedTokenRing.Keys.Count || i < 0)
                        i = 0;
                }

                return _permittedTokenRing[_permittedTokenRing.Keys.ElementAt(i)];
            }

            /// <summary>
            /// Get number of permitted tokens
            /// </summary>
            /// <remarks>
            /// Should be number of nodes * vnodes per node
            /// </remarks>
            /// <returns>Number of permitted tokens in ring</returns>
            internal int ringSize()
            {
                return _permittedTokenRing.Count;
            }

            /// <summary>
            /// Adds a new range of tokens to the permitted token ring
            /// </summary>
            /// <param name="peer">IPAddress and token range of the discovered node</param>
            internal void addOrUpdateNode(Peer peer)
            {
                foreach (BigInteger token in peer.Tokens)
                {
                    //remove if updating
                    _permittedTokenRing.Remove(token);
                    _bannedTokenRing.Remove(token);

                    _permittedTokenRing.Add(token, peer.RpcAddress);

                    _tokenCache = _permittedTokenRing.Keys.ToList();
                }
            }

            /// <summary>
            /// Move keys from one dictionary to another based on their value's
            /// </summary>
            /// <typeparam name="T">Type of key</typeparam>
            /// <typeparam name="V">Type of value</typeparam>
            /// <param name="src">Source Dictionary</param>
            /// <param name="dest">Destination Dictionary</param>
            /// <param name="val">Value to move by</param>
            private void moveOnValue<T, V>(IDictionary<T, V> src, IDictionary<T, V> dest, V val)
            {
                var bannedTokens = src.Where(keyVal => keyVal.Value.Equals(val));
                foreach (var token in bannedTokens)
                {
                    dest.Add(token.Key, token.Value);
                    src.Remove(token.Key);
                }
            }

            /// <summary>
            /// Remove keys from a dictionary based on their value's
            /// </summary>
            /// <typeparam name="T">Type of key</typeparam>
            /// <typeparam name="V">Type of value</typeparam>
            /// <param name="src">Source Dictionary</param>
            /// <param name="val">Value to remove by</param>
            private void removeOnValue<T, V>(IDictionary<T, V> src, V val)
            {
                foreach(var keyVal in src.Where(keyVal => keyVal.Value.Equals(val)).ToArray())
                    src.Remove(keyVal.Key);
            }

            /// <summary>
            /// Ban a node's tokens
            /// </summary>
            /// <param name="endpoint">Node to ban</param>
            internal void banNode(IPAddress endpoint)
            {
                moveOnValue(_permittedTokenRing, _bannedTokenRing, endpoint);
                _tokenCache = _permittedTokenRing.Keys.ToList();
            }

            /// <summary>
            /// Permit a node's tokens
            /// </summary>
            /// <param name="endpoint">Node to permit</param>
            internal void permitNode(IPAddress endpoint)
            {
                moveOnValue(_bannedTokenRing, _permittedTokenRing, endpoint);
                _tokenCache = _permittedTokenRing.Keys.ToList();
            }

            /// <summary>
            /// Completely remove a node's tokens from the ring
            /// </summary>
            /// <param name="endpoint">Node to remove</param>
            internal void removeNode(IPAddress endpoint)
            {
                removeOnValue(_permittedTokenRing, endpoint);
                removeOnValue(_bannedTokenRing, endpoint);
                _tokenCache = _permittedTokenRing.Keys.ToList();
            }
        }
    }
}
