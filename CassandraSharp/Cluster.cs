namespace CassandraSharp
{
    using System;
    using System.IO;
    using System.Net.Sockets;
    using Apache.Cassandra;
    using CassandraSharp.Commands;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Pool;
    using CassandraSharp.Transport;
    using CassandraSharp.Utils;
    using log4net;
    using Thrift.Protocol;
    using Thrift.Transport;

    internal class Cluster : ICluster
    {
        private readonly IEndpointStrategy _endpointsManager;

        private readonly ILog _logger = LogManager.GetLogger(typeof(Cluster));

        private readonly IPool<IConnection> _pool;

        private readonly ITransportFactory _transportFactory;

        public Cluster(IBehaviorConfig behaviorConfig, IPool<IConnection> pool, ITransportFactory transportFactory, IEndpointStrategy endpointsManager)
        {
            BehaviorConfig = behaviorConfig;
            _pool = pool;
            _endpointsManager = endpointsManager;
            _transportFactory = transportFactory;
        }

        public IBehaviorConfig BehaviorConfig { get; private set; }

        public void Dispose()
        {
            _pool.SafeDispose();
        }

        public TResult ExecuteCommand<TResult>(IBehaviorConfig behaviorConfig, Func<Cassandra.Client, TResult> func)
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
                    connection = AcquireConnection();

                    OpenConnection(connection, behaviorConfig);
                    TResult res = func(connection.CassandraClient);

                    ReleaseConnection(connection, false);

                    return res;
                }
                catch (Exception ex)
                {
                    bool connectionDead;
                    bool retry;
                    DecipherException(ex, behaviorConfig, out connectionDead, out retry);

                    string errMsg = string.Format("Exception during command processing: connectionDead={0} retry={1}", connectionDead, retry);
                    _logger.Error(errMsg, ex);

                    ReleaseConnection(connection, connectionDead);
                    if (!retry || tryCount >= behaviorConfig.MaxRetries)
                    {
                        _logger.Fatal("Max retry count reached");
                        throw;
                    }
                }

                ++tryCount;
            }
        }

        private static void OpenConnection(IConnection connection, IBehaviorConfig behaviorConfig)
        {
            TTransport transport = connection.CassandraClient.InputProtocol.Transport;
            if (!transport.IsOpen)
            {
                transport.Open();
            }

            bool userChanged = connection.User != behaviorConfig.User;
            if (userChanged && null != behaviorConfig.User && null != behaviorConfig.Password)
            {
                SystemManagement.Login(connection.CassandraClient, behaviorConfig.User, behaviorConfig.Password);
            }

            bool keyspaceChanged = connection.KeySpace != behaviorConfig.KeySpace;
            if (keyspaceChanged && null != behaviorConfig.KeySpace)
            {
                SystemManagement.SetKeySpace(connection.CassandraClient, behaviorConfig.KeySpace);
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

        private IConnection AcquireConnection()
        {
            IConnection connection;
            if (_pool.Acquire(out connection))
            {
                return connection;
            }

            TTransport transport = null;
            try
            {
                Endpoint endpoint = _endpointsManager.Pick();
                if (null == endpoint)
                {
                    throw new ArgumentException("Can't find any valid endpoint");
                }

                transport = _transportFactory.Create(endpoint.Address);
                TProtocol protocol = new TBinaryProtocol(transport);
                Cassandra.Client client = new Cassandra.Client(protocol);

                connection = new Connection(client, endpoint);
                return connection;
            }
            catch
            {
                if (null != transport)
                {
                    transport.Close();
                }
                throw;
            }
        }

        private void ReleaseConnection(IConnection connection, bool hasFailed)
        {
            // protect against exception during acquire connection
            if (null != connection)
            {
                if (hasFailed)
                {
                    _endpointsManager.Ban(connection.Endpoint);
                    connection.SafeDispose();
                }
                else
                {
                    _pool.Release(connection);
                }
            }
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