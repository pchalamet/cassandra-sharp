namespace CassandraSharp
{
    using System;
    using Apache.Cassandra;

    public static class ConnectionInfoExtensions
    {
        public static ICluster Overrides(this IConnectionInfo @this, ICluster cluster)
        {
            return new ClusterOverride(cluster, @this);
        }

        private struct ClusterOverride : ICluster
        {
            private readonly ICluster _cluster;

            private readonly IConnectionInfo _cnxInfo;

            public ClusterOverride(ICluster cluster, IConnectionInfo cnxInfo)
            {
                _cluster = cluster;
                _cnxInfo = cnxInfo;
            }

            public void Dispose()
            {
            }

            public IConnectionInfo DefaultConnectionInfo
            {
                get { return _cnxInfo; }
            }

            public ConsistencyLevel DefaultReadConsistencyLevel
            {
                get { return _cluster.DefaultReadConsistencyLevel; }
            }

            public ConsistencyLevel DefaultWriteConsistencyLevel
            {
                get { return _cluster.DefaultWriteConsistencyLevel; }
            }

            public int DefaultTTL
            {
                get { return _cluster.DefaultTTL; }
            }

            public TResult ExecuteCommand<TResult>(IConnectionInfo cnxInfo, Func<Cassandra.Client, TResult> func)
            {
                return _cluster.ExecuteCommand(_cnxInfo, func);
            }
        }
    }
}