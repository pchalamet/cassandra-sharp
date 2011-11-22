namespace CassandraSharp
{
    using Apache.Cassandra;

    public interface IBehaviorConfig
    {
        string KeySpace { get; }

        ConsistencyLevel ReadConsistencyLevel { get; }

        int TTL { get; }

        ConsistencyLevel WriteConsistencyLevel { get; }

        int MaxRetries { get; }

        string Password { get; }

        bool RetryOnNotFound { get; }

        bool RetryOnTimeout { get; }

        bool RetryOnUnavailable { get; }

        string User { get; }
    }
}