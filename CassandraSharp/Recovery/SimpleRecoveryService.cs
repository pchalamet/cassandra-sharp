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

namespace CassandraSharp.Recovery
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Timers;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal class SimpleRecoveryService : IRecoveryService
    {
        private readonly object _lock;

        private readonly Timer _timer;

        private readonly List<RecoveryItem> _toRecover;

        public SimpleRecoveryService()
        {
            _toRecover = new List<RecoveryItem>();
            _timer = new Timer(60*1000);
            _timer.Elapsed += (s, e) => TryRecover();
            _lock = new object();
        }

        public void Recover(IPAddress endpoint, IConnectionFactory connectionFactory, Action<IConnection> clientRecoveredCallback)
        {
            lock (_lock)
            {
                RecoveryItem recoveryItem = new RecoveryItem(endpoint, connectionFactory, clientRecoveredCallback);
                _toRecover.Add(recoveryItem);

                _timer.Enabled = true;
            }
        }

        public void Dispose()
        {
            _timer.SafeDispose();
        }

        private void TryRecover()
        {
            List<RecoveryItem> toRecover;
            lock (_lock)
            {
                toRecover = new List<RecoveryItem>(_toRecover);
            }

            foreach (RecoveryItem recoveryItem in toRecover)
            {
                try
                {
                    IConnection client = recoveryItem.ConnectionFactory.Create(recoveryItem.Endpoint);

                    lock (_lock)
                    {
                        _toRecover.Remove(recoveryItem);
                    }

                    recoveryItem.ClientRecoveredCallback(client);
                }
// ReSharper disable EmptyGeneralCatchClause
                catch
// ReSharper restore EmptyGeneralCatchClause
                {
                }
            }

            lock (_lock)
            {
                _timer.Enabled = 0 < _toRecover.Count;
            }
        }

        private class RecoveryItem
        {
            public RecoveryItem(IPAddress endpoint, IConnectionFactory connectionFactory, Action<IConnection> clientRecoveredCallback)
            {
                Endpoint = endpoint;
                ConnectionFactory = connectionFactory;
                ClientRecoveredCallback = clientRecoveredCallback;
            }

            public IPAddress Endpoint { get; private set; }

            public IConnectionFactory ConnectionFactory { get; private set; }

            public Action<IConnection> ClientRecoveredCallback { get; private set; }
        }
    }
}