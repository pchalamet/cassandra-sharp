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

namespace CassandraSharp.Recovery
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Timers;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal sealed class AttemptConnectRecoveryService : IRecoveryService
    {
        private readonly object _lock;

        private readonly ILogger _logger;

        private readonly Timer _timer;

        private readonly List<RecoveryItem> _toRecover;

        public AttemptConnectRecoveryService(ILogger logger, RecoveryConfig config)
        {
            _logger = logger;
            _toRecover = new List<RecoveryItem>();
            _timer = new Timer(config.Interval * 1000);
            _timer.Elapsed += (s, e) => TryRecover();
            _timer.AutoReset = false;
            _lock = new object();
        }

        public void Recover(IPAddress endpoint, IConnectionFactory connectionFactory, Action<IConnection> clientRecoveredCallback)
        {
            lock (_lock)
            {
                RecoveryItem recoveryItem = new RecoveryItem(endpoint, connectionFactory, clientRecoveredCallback);
                _toRecover.Add(recoveryItem);
                _timer.Start();
            }
        }

        public void Dispose()
        {
            _timer.SafeDispose();
        }

        private void TryRecover()
        {
            _logger.Debug("Trying to recover endpoints");

            List<RecoveryItem> toRecover;
            lock (_lock)
            {
                toRecover = new List<RecoveryItem>(_toRecover);
            }

            foreach (RecoveryItem recoveryItem in toRecover)
            {
                try
                {
                    _logger.Debug("Trying to recover endpoint {0}", recoveryItem.Endpoint);
                    IConnection client = recoveryItem.ConnectionFactory.Create(recoveryItem.Endpoint);
                    _logger.Debug("Endpoint {0} successfully recovered", recoveryItem.Endpoint);

                    lock (_lock)
                    {
                        _toRecover.Remove(recoveryItem);
                    }

                    recoveryItem.ClientRecoveredCallback(client);
                }
                catch(Exception ex)
                {
                    _logger.Debug("Failed to recover endpoint {0} with error {1}", recoveryItem.Endpoint, ex);
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