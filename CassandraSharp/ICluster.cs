namespace CassandraSharp
{
    using System;
    using Apache.Cassandra;

    /// <summary>
    ///     Primary interface to execute commands against a Cassandra cluster
    ///     Implementation of this interface must be thread safe
    /// </summary>
    public interface ICluster : IDisposable
    {
        IConnectionInfo DefaultConnectionInfo { get; }

        ConsistencyLevel DefaultReadConsistencyLevel { get; }

        ConsistencyLevel DefaultWriteConsistencyLevel { get; }

        int DefaultTTL { get; }

        TResult ExecuteCommand<TResult>(IConnectionInfo cnxInfo, Func<Cassandra.Client, TResult> func);
    }
}