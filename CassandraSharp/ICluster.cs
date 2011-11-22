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
        IBehaviorConfig BehaviorConfig { get; }

        TResult ExecuteCommand<TResult>(IBehaviorConfig behaviorConfig, Func<Cassandra.Client, TResult> func);
    }
}