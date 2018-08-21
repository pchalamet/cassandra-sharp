// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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
using System.Linq;
using System.Net;
using System.Numerics;
using CassandraSharp.Extensibility;

namespace CassandraSharp.Utils
{
    /// <summary>
    ///     Represents spread of tokens around nodes
    /// </summary>
    internal sealed class TokenRing
    {
        private readonly SortedDictionary<BigInteger, IPAddress> _bannedTokenRing;

        private readonly SortedDictionary<BigInteger, IPAddress> _permittedTokenRing;

        private List<BigInteger> _tokenCache; //sorted list of tokens: for efficiency of binary search

        internal TokenRing()
        {
            _permittedTokenRing = new SortedDictionary<BigInteger, IPAddress>();
            _bannedTokenRing = new SortedDictionary<BigInteger, IPAddress>();
            _tokenCache = new List<BigInteger>();
        }

        /// <summary>
        ///     Use binary search to find first token that is smaller than key
        /// </summary>
        /// <param name="key">Row key as BigInteger hash</param>
        /// <returns>IPAddress of node that owns the token</returns>
        internal IPAddress FindReplica(BigInteger key)
        {
            var i = _tokenCache.BinarySearch(key);

            if (i < 0)
            {
                i = ~i - 1;
                if (i >= _permittedTokenRing.Keys.Count || i < 0) i = 0;
            }

            return _permittedTokenRing[_permittedTokenRing.Keys.ElementAt(i)];
        }

        /// <summary>
        ///     Get number of permitted tokens
        /// </summary>
        /// <remarks>
        ///     Should be number of nodes * vnodes per node
        /// </remarks>
        /// <returns>Number of permitted tokens in ring</returns>
        internal int RingSize()
        {
            return _permittedTokenRing.Count;
        }

        /// <summary>
        ///     Adds a new range of tokens to the permitted token ring
        /// </summary>
        /// <param name="peer">IPAddress and token range of the discovered node</param>
        internal void AddOrUpdateNode(Peer peer)
        {
            foreach (var token in peer.Tokens)
            {
                //remove if updating
                _permittedTokenRing.Remove(token);
                _bannedTokenRing.Remove(token);

                _permittedTokenRing.Add(token, peer.RpcAddress);

                _tokenCache = _permittedTokenRing.Keys.ToList();
            }
        }

        /// <summary>
        ///     Move keys from one dictionary to another based on their value's
        /// </summary>
        /// <param name="src">Source Dictionary</param>
        /// <param name="dest">Destination Dictionary</param>
        /// <param name="val">Value to move by</param>
        private static void MoveOnValue(IDictionary<BigInteger, IPAddress> src, IDictionary<BigInteger, IPAddress> dest, IPAddress val)
        {
            var bannedTokens = src.Where(keyVal => keyVal.Value.Equals(val));
            foreach (var token in bannedTokens)
            {
                dest.Add(token.Key, token.Value);
                src.Remove(token.Key);
            }
        }

        /// <summary>
        ///     Remove keys from a dictionary based on their value's
        /// </summary>
        /// <param name="src">Source Dictionary</param>
        /// <param name="val">Value to remove by</param>
        private static void RemoveOnValue(IDictionary<BigInteger, IPAddress> src, IPAddress val)
        {
            foreach (var keyVal in src.Where(keyVal => keyVal.Value.Equals(val)).ToArray()) src.Remove(keyVal.Key);
        }

        /// <summary>
        ///     Ban a node's tokens
        /// </summary>
        /// <param name="endpoint">Node to ban</param>
        internal void BanNode(IPAddress endpoint)
        {
            MoveOnValue(_permittedTokenRing, _bannedTokenRing, endpoint);
            _tokenCache = _permittedTokenRing.Keys.ToList();
        }

        /// <summary>
        ///     Permit a node's tokens
        /// </summary>
        /// <param name="endpoint">Node to permit</param>
        internal void PermitNode(IPAddress endpoint)
        {
            MoveOnValue(_bannedTokenRing, _permittedTokenRing, endpoint);
            _tokenCache = _permittedTokenRing.Keys.ToList();
        }

        /// <summary>
        ///     Completely remove a node's tokens from the ring
        /// </summary>
        /// <param name="endpoint">Node to remove</param>
        internal void RemoveNode(IPAddress endpoint)
        {
            RemoveOnValue(_permittedTokenRing, endpoint);
            RemoveOnValue(_bannedTokenRing, endpoint);
            _tokenCache = _permittedTokenRing.Keys.ToList();
        }
    }
}