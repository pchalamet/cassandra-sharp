namespace CassandraSharp
{
    using System;
    using Apache.Cassandra;

    public static class CommandInfoExtensions
    {
        public static ICluster Configure(this ICluster @this, BehaviorConfigBuilder cmdInfoBuilder)
        {
            IBehaviorConfig behaviorConfig = cmdInfoBuilder.Build(@this.BehaviorConfig);
            return new ConfiguredCluster(@this, behaviorConfig);
        }

        private class ConfiguredCluster : ICluster
        {
            private readonly ICluster _cluster;

            public ConfiguredCluster(ICluster cluster, IBehaviorConfig behaviorConfig)
            {
                _cluster = cluster;
                BehaviorConfig = behaviorConfig;
            }

            public void Dispose()
            {
            }

            public IBehaviorConfig BehaviorConfig { get; private set; }

            public TResult ExecuteCommand<TResult>(IBehaviorConfig behaviorConfig, Func<Cassandra.Client, TResult> func)
            {
                return _cluster.ExecuteCommand(BehaviorConfig, func);
            }
        }
    }
}