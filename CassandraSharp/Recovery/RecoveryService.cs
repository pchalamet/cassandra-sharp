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
    using System.Timers;
    using Apache.Cassandra;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Utils;

    internal class RecoveryService : IRecoveryService
    {
        private readonly object _lock;

        private readonly Timer _timer;

        private readonly List<RecoveryItem> _toRecover;

        public RecoveryService()
        {
            _toRecover = new List<RecoveryItem>();
            _timer = new Timer(60*1000);
            _timer.Elapsed += TryRecover;
            _lock = new object();
        }

        public void Recover(Endpoint endpoint, ITransportFactory transportFactory, Action<Endpoint, Cassandra.Client> clientRecoveredCallback)
        {
            lock (_lock)
            {
                RecoveryItem recoveryItem = new RecoveryItem(endpoint, transportFactory, clientRecoveredCallback);
                _toRecover.Add(recoveryItem);

                _timer.Enabled = true;
            }
        }

        public void Dispose()
        {
            _timer.Enabled = false;
            _timer.SafeDispose();
        }

        private void TryRecover(object sender, ElapsedEventArgs e)
        {
            List<RecoveryItem> toRecover;
            lock (_lock)
            {
                toRecover = new List<RecoveryItem>(_toRecover);
            }

            foreach (RecoveryItem recoveryItem in toRecover)
            {
                Cassandra.Client client = null;
                try
                {
                    client = recoveryItem.TransportFactory.Create(recoveryItem.Endpoint.Address);

                    lock (_lock)
                    {
                        _toRecover.Remove(recoveryItem);
                    }

                    recoveryItem.ClientRecoveredCallback(recoveryItem.Endpoint, client);
                }
// ReSharper disable EmptyGeneralCatchClause
                catch
// ReSharper restore EmptyGeneralCatchClause
                {
                }
                finally
                {
                    if (null != client && client.OutputProtocol.Transport.IsOpen)
                    {
                        client.OutputProtocol.Transport.Close();
                    }
                }
            }

            lock (_lock)
            {
                _timer.Enabled = 0 < _toRecover.Count;
            }
        }

        private class RecoveryItem
        {
            public RecoveryItem(Endpoint endpoint, ITransportFactory transportFactory, Action<Endpoint, Cassandra.Client> clientRecoveredCallback)
            {
                Endpoint = endpoint;
                TransportFactory = transportFactory;
                ClientRecoveredCallback = clientRecoveredCallback;
            }

            public Endpoint Endpoint { get; private set; }

            public ITransportFactory TransportFactory { get; private set; }

            public Action<Endpoint, Cassandra.Client> ClientRecoveredCallback { get; private set; }
        }
    }
}