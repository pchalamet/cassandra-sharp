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

namespace CassandraSharp.Implementation
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

        private readonly IPool<IConnection> _pool;

        private readonly IRecoveryService _recoveryService;

        private readonly ITransportFactory _transportFactory;

        public Cluster(IBehaviorConfig behaviorConfig, IPool<IConnection> pool, ITransportFactory transportFactory, IEndpointStrategy endpointsManager,
                       IRecoveryService recoveryService, ITimestampService timestampService, ILog logger)
        {
            BehaviorConfig = behaviorConfig;
            _pool = pool;
            _endpointsManager = endpointsManager;
            _recoveryService = recoveryService;
            TimestampService = timestampService;
            _transportFactory = transportFactory;
            _logger = logger;
        }

        public IBehaviorConfig BehaviorConfig { get; private set; }

        public ITimestampService TimestampService { get; private set; }

        public void Dispose()
        {
            _pool.SafeDispose();
        }

        public TResult ExecuteCommand<TResult>(IBehaviorConfig behaviorConfig, Func<IConnection, TResult> func, Func<byte[]> keyFunc)
        {
            if (null == behaviorConfig)
            {
                behaviorConfig = BehaviorConfig;
            }

            int tryCount = 1;
            while (true)
            {
                IConnection connection = null;
                try
                {
                    byte[] key = null;
                    if( null != keyFunc)
                    {
                        key = keyFunc();
                    }

                    connection = AcquireConnection(key);

                    ChangeKeyspace(connection, behaviorConfig);
                    TResult res = func(connection);

                    ReleaseConnection(connection, false);

                    return res;
                }
                catch (Exception ex)
                {
                    bool connectionDead;
                    bool retry;
                    DecipherException(ex, behaviorConfig, out connectionDead, out retry);

                    _logger.Error("Exception during command processing: connectionDead={0} retry={1} : {2}", connectionDead, retry, ex.Message);

                    ReleaseConnection(connection, connectionDead);
                    if (!retry || tryCount >= behaviorConfig.MaxRetries)
                    {
                        _logger.Fatal("Max retry count reached");
                        throw;
                    }

                    Thread.Sleep(behaviorConfig.SleepBeforeRetry);
                }

                ++tryCount;
            }
        }

        private static void ChangeKeyspace(IConnection connection, IBehaviorConfig behaviorConfig)
        {
            bool keyspaceChanged = connection.KeySpace != behaviorConfig.KeySpace;
            if (keyspaceChanged && null != behaviorConfig.KeySpace)
            {
                connection.CassandraClient.set_keyspace(behaviorConfig.KeySpace);
                connection.KeySpace = behaviorConfig.KeySpace;
            }
        }

        private static void DecipherException(Exception ex, IBehaviorConfig behaviorConfig, out bool connectionDead, out bool retry)
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
                retry = behaviorConfig.RetryOnTimeout;
            }
            else if (ex is UnavailableException)
            {
                connectionDead = false;
                retry = behaviorConfig.RetryOnUnavailable;
            }
            else if (ex is NotFoundException)
            {
                connectionDead = false;
                retry = behaviorConfig.RetryOnNotFound;
            }

                // other exceptions ==> connection is not dead / do not retry
            else
            {
                connectionDead = false;
                retry = false;
            }
        }

        private IConnection AcquireConnection(byte[] key)
        {
            IConnection connection;
            if (_pool.Acquire(out connection))
            {
                return connection;
            }

            Endpoint endpoint = _endpointsManager.Pick(key);
            if (null == endpoint)
            {
                throw new ArgumentException("Can't find any valid endpoint");
            }

            Cassandra.Client client = _transportFactory.Create(endpoint.Address);
            connection = new Connection(client, endpoint);
            return connection;
        }

        private void ReleaseConnection(IConnection connection, bool hasFailed)
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