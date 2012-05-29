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

namespace CassandraSharp
{
    using System;
    using System.IO;
    using System.Net.Sockets;
    using System.Threading;
    using Apache.Cassandra;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Utils;
    using Thrift.Transport;

    internal class Cluster : ICluster
    {
        private readonly IEndpointStrategy _endpointsManager;

        private readonly ILog _logger;

        private readonly ICluster _parentCluster;

        private readonly IPool<IConnection> _pool;

        private readonly IRecoveryService _recoveryService;

        private readonly ITransportFactory _transportFactory;

        public Cluster(IBehaviorConfig behaviorConfig, IPool<IConnection> pool, ITransportFactory transportFactory, IEndpointStrategy endpointsManager,
                       IRecoveryService recoveryService, ITimestampService timestampService, ILog logger)
            : this(behaviorConfig, pool, transportFactory, endpointsManager, recoveryService, timestampService, logger, null)
        {
        }

        private Cluster(IBehaviorConfig behaviorConfig, IPool<IConnection> pool, ITransportFactory transportFactory, IEndpointStrategy endpointsManager,
                        IRecoveryService recoveryService, ITimestampService timestampService, ILog logger, ICluster parentCluster)
        {
            BehaviorConfig = behaviorConfig;
            TimestampService = timestampService;
            _pool = pool;
            _endpointsManager = endpointsManager;
            _recoveryService = recoveryService;
            _transportFactory = transportFactory;
            _logger = logger;
            _parentCluster = parentCluster;
        }

        public void Dispose()
        {
            if (null == _parentCluster)
            {
                _pool.SafeDispose();
            }
        }

        public IBehaviorConfig BehaviorConfig { get; private set; }

        public ITimestampService TimestampService { get; private set; }

        public TResult ExecuteCommand<TResult>(Func<IConnection, TResult> func, Func<byte[]> keyFunc)
        {
            int tryCount = 1;
            while (true)
            {
                IConnection connection = null;
                try
                {
                    byte[] key = null;
                    if (null != keyFunc)
                    {
                        key = keyFunc();
                    }

                    connection = AcquireConnection(key);

                    TResult res = func(connection);

                    ReleaseConnection(connection, false);

                    return res;
                }
                catch (Exception ex)
                {
                    bool connectionDead;
                    bool retry;
                    DecipherException(ex, out connectionDead, out retry);

                    _logger.Error("Exception during command processing: connectionDead={0} retry={1} : {2}", connectionDead, retry, ex.Message);

                    ReleaseConnection(connection, connectionDead);
                    if (!retry || tryCount >= BehaviorConfig.MaxRetries)
                    {
                        _logger.Fatal("Max retry count reached");
                        throw;
                    }

                    if (BehaviorConfig.SleepBeforeRetry.HasValue)
                    {
                        Thread.Sleep(BehaviorConfig.SleepBeforeRetry.Value);
                    }
                }

                ++tryCount;
            }
        }

        public ICluster CreateChildCluster(IBehaviorConfig cfgOverride)
        {
            IBehaviorConfig childConfig = cfgOverride.Override(BehaviorConfig);
            return new Cluster(childConfig, _pool, _transportFactory, _endpointsManager, _recoveryService, TimestampService, _logger, this);
        }

        public IConnection AcquireConnection(byte[] key)
        {
            IConnection connection;
            if (!_pool.Acquire(out connection))
            {
                Endpoint endpoint = _endpointsManager.Pick(key);
                if (null == endpoint)
                {
                    throw new ArgumentException("Can't find any valid endpoint");
                }

                Cassandra.Client client = _transportFactory.Create(endpoint.Address);
                connection = new Connection(client, endpoint);
            }

            ChangeKeyspace(connection);

            return connection;
        }

        public void ReleaseConnection(IConnection connection, bool hasFailed)
        {
            // protect against exception during acquire connection
            if (null != connection)
            {
                if (hasFailed)
                {
                    if (null != _recoveryService)
                    {
                        _logger.Info("marking {0} for recovery", connection.Endpoint.Address);
                        _recoveryService.Recover(connection.Endpoint, _transportFactory, ClientRecoveredCallback);
                    }

                    _endpointsManager.Ban(connection.Endpoint);
                    connection.SafeDispose();
                }
                else
                {
                    _pool.Release(connection);
                }
            }
        }

        private void ChangeKeyspace(IConnection connection)
        {
            bool keyspaceChanged = connection.KeySpace != BehaviorConfig.KeySpace;
            if (keyspaceChanged && null != BehaviorConfig.KeySpace)
            {
                connection.CassandraClient.set_keyspace(BehaviorConfig.KeySpace);
                connection.KeySpace = BehaviorConfig.KeySpace;

                // UNDONE: CQL3 seems to be badly broken - USE does not work actually
                //string useKeyspace = string.Format("use {0}", connection.KeySpace);
                //byte[] bufUseKeyspace = new Utf8NameOrValue(useKeyspace).ToByteArray();
                //connection.CassandraClient.execute_cql_query(bufUseKeyspace, Compression.NONE);
            }
        }

        private void DecipherException(Exception ex, out bool connectionDead, out bool retry)
        {
            // connection dead exception handling
            if (ex is TTransportException)
            {
                connectionDead = true;
                retry = true;
            }
            else if (ex is IOException)
            {
                connectionDead = true;
                retry = true;
            }
            else if (ex is SocketException)
            {
                connectionDead = true;
                retry = true;
            }

                // functional exception handling
            else if (ex is TimedOutException)
            {
                connectionDead = false;
                retry = BehaviorConfig.RetryOnTimeout ?? false;
            }
            else if (ex is UnavailableException)
            {
                connectionDead = false;
                retry = BehaviorConfig.RetryOnUnavailable ?? false;
            }
            else if (ex is NotFoundException)
            {
                connectionDead = false;
                retry = BehaviorConfig.RetryOnNotFound ?? false;
            }

                // other exceptions ==> connection is not dead / do not retry
            else
            {
                connectionDead = false;
                retry = false;
            }
        }

        private void ClientRecoveredCallback(Endpoint endpoint, Cassandra.Client client)
        {
            _logger.Info("{0} is recovered", endpoint.Address);

            _endpointsManager.Permit(endpoint);
            Connection connection = new Connection(client, endpoint);
            _pool.Release(connection);
        }

        private class Connection : IConnection
        {
            public Connection(Cassandra.Client client, Endpoint endpoint)
            {
                Endpoint = endpoint;
                CassandraClient = client;
            }

            public void Dispose()
            {
                CassandraClient.InputProtocol.Transport.Close();
            }

            public string KeySpace { get; set; }

            public string User { get; set; }

            public Endpoint Endpoint { get; private set; }

            public Cassandra.Client CassandraClient { get; private set; }
        }
    }
}