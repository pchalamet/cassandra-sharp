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

namespace CassandraSharp.Pool
{
    using System.Net;
    using Apache.Cassandra;

    internal class PooledConnection : IConnection
    {
        private readonly IPool<Token, IConnection> _pool;

        private readonly Token _token;

        private bool _keepAlive;

        public PooledConnection(Cassandra.Client client, IPAddress endpoint, Token token, IPool<Token, IConnection> pool)
        {
            _token = token;
            _pool = pool;
            Endpoint = endpoint;
            CassandraClient = client;
        }

        public void Dispose()
        {
            if (_keepAlive)
            {
                _keepAlive = false;
                _pool.Release(_token, this);
            }
            else
            {
                CassandraClient.InputProtocol.Transport.Close();
            }
        }

        public void KeepAlive()
        {
            _keepAlive = true;
        }

        public string KeySpace { get; set; }

        public string User { get; set; }

        public IPAddress Endpoint { get; private set; }

        public Cassandra.Client CassandraClient { get; private set; }
    }
}