namespace CassandraSharp
{
    using Apache.Cassandra;
    using CassandraSharp.Config;

    public class BehaviorConfigBuilder
    {
        public string KeySpace { get; set; }

        public string Login { get; set; }

        public string Password { get; set; }

        public ConsistencyLevel? ReadConsistencyLevel { get; set; }

        public ConsistencyLevel? WriteConsistencyLevel { get; set; }

        public int TTL { get; set; }

        public bool? RetryOnNotFound { get; set; }

        public bool? RetryOnTimeout { get; set; }

        public bool? RetryOnUnavailable { get; set; }

        public IBehaviorConfig Build(IBehaviorConfig behaviorConfigToOverride)
        {
            IBehaviorConfig behaviorConfigConfig = new BehaviorConfig
                                                       {
                                                           KeySpace = KeySpace ?? behaviorConfigToOverride.KeySpace,
                                                           User = Login ?? behaviorConfigToOverride.User,
                                                           Password = Password ?? behaviorConfigToOverride.Password,
                                                           ReadConsistencyLevel = ReadConsistencyLevel ?? behaviorConfigToOverride.ReadConsistencyLevel,
                                                           WriteConsistencyLevel = WriteConsistencyLevel ?? behaviorConfigToOverride.WriteConsistencyLevel,
                                                           TTL = behaviorConfigToOverride.TTL,
                                                           RetryOnNotFound = RetryOnNotFound ?? behaviorConfigToOverride.RetryOnNotFound,
                                                           RetryOnTimeout = RetryOnTimeout ?? behaviorConfigToOverride.RetryOnTimeout,
                                                           RetryOnUnavailable = RetryOnUnavailable ?? behaviorConfigToOverride.RetryOnUnavailable
                                                       };
            return behaviorConfigConfig;
        }
    }
}