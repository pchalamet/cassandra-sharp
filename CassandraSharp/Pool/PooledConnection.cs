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