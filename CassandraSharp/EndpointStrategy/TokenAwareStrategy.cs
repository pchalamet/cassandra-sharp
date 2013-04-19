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
    using CassandraSharp.Utils;

    internal class TokenAwareStrategy : IEndpointStrategy
    {
        private readonly IComparer<Partition> _comparer = new TokenComparer();

        private readonly object _lock = new object();

        private readonly Partition[] _partitions = new Partition[0];

        private readonly Dictionary<IPAddress, bool> _stateEndpoints = new Dictionary<IPAddress, bool>();

        public TokenAwareStrategy(IEnumerable<IPAddress> endpoints)
        {
            foreach (IPAddress endpoint in endpoints)
            {
                _stateEndpoints[endpoint] = true;
            }
        }

        public IPAddress Pick(QueryHint hint)
        {
            if (null == hint)
            {
                throw new ArgumentException("hint");
            }

            // TODO: convert Key to BigInteger
            BigInteger tokenValue = 0;

            Partition tmpPartition = new Partition {Token = tokenValue};
            lock (_lock)
            {
                int idx = Array.BinarySearch(_partitions, tmpPartition, _comparer);
                if (idx < 0)
                {
                    idx = ~idx;
                }

                int rf = hint.ReplicationFactor;
                while (rf > 0)
                {
                    idx = idx % _partitions.Length;
                    IPAddress endpoint = _partitions[idx].Node;
                    if (_stateEndpoints[endpoint])
                    {
                        return endpoint;
                    }

                    --rf;
                    ++idx;
                }

                return null;
            }
        }

        public void Ban(IPAddress endpoint)
        {
            lock (_lock)
                _stateEndpoints[endpoint] = false;
        }

        public void Permit(IPAddress endpoint)
        {
            lock (_lock)
                _stateEndpoints[endpoint] = true;
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            lock (_lock)
            {
                switch (kind)
                {
                    case NotificationKind.Add:
                    case NotificationKind.Update:
                        IPAddress endpoint = peer.RpcAddress;
                        if (!_stateEndpoints.ContainsKey(endpoint))
                        {
                            _stateEndpoints[endpoint] = true;
                        }

                        foreach (BigInteger token in peer.Tokens)
                        {
                            Partition newPartition = new Partition {Node = endpoint, Token = token};
                            _partitions.BinaryAdd(newPartition, _comparer);
                        }
                        break;

                    case NotificationKind.Remove:
                        foreach (BigInteger token in peer.Tokens)
                        {
                            Partition newPartition = new Partition {Token = token};
                            _partitions.BinaryRemove(newPartition, _comparer);
                        }
                        break;
                }
            }
        }

        private struct Partition
        {
            public IPAddress Node;

            public BigInteger Token;
        }

        private class TokenComparer : IComparer<Partition>
        {
            public int Compare(Partition x, Partition y)
            {
                return x.Token.CompareTo(y);
            }
        }
    }
}