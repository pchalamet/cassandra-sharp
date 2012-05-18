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
    using Apache.Cassandra;
    using CassandraSharp.Config;

    public class BehaviorConfigBuilder
    {
        public string KeySpace { get; set; }

        public int? MaxRetries { get; set; }

        public ConsistencyLevel? ReadConsistencyLevel { get; set; }

        public ConsistencyLevel? WriteConsistencyLevel { get; set; }

        public int? TTL { get; set; }

        public bool? RetryOnNotFound { get; set; }

        public bool? RetryOnTimeout { get; set; }

        public bool? RetryOnUnavailable { get; set; }

        public int? SleepBeforeRetry { get; set; }

        private IBehaviorConfig OverrideConfig(IBehaviorConfig behaviorConfigToOverride)
        {
            IBehaviorConfig behaviorConfigConfig = new BehaviorConfig
                                                       {
                                                           KeySpace = KeySpace ?? behaviorConfigToOverride.KeySpace,
                                                           MaxRetries = MaxRetries ?? behaviorConfigToOverride.MaxRetries,
                                                           ReadConsistencyLevel = ReadConsistencyLevel ?? behaviorConfigToOverride.ReadConsistencyLevel,
                                                           WriteConsistencyLevel = WriteConsistencyLevel ?? behaviorConfigToOverride.WriteConsistencyLevel,
                                                           TTL = TTL ?? behaviorConfigToOverride.TTL,
                                                           RetryOnNotFound = RetryOnNotFound ?? behaviorConfigToOverride.RetryOnNotFound,
                                                           RetryOnTimeout = RetryOnTimeout ?? behaviorConfigToOverride.RetryOnTimeout,
                                                           RetryOnUnavailable = RetryOnUnavailable ?? behaviorConfigToOverride.RetryOnUnavailable,
                                                           SleepBeforeRetry = SleepBeforeRetry ?? behaviorConfigToOverride.SleepBeforeRetry
                                                       };

            return behaviorConfigConfig;
        }

        public ICluster Build(ICluster cluster)
        {
            IBehaviorConfig behaviorConfig = OverrideConfig(cluster.BehaviorConfig);
            return new ConfiguredCluster(cluster, behaviorConfig);
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

            public ITimestampService TimestampService { get; private set; }

            public TResult ExecuteCommand<TResult>(IBehaviorConfig behaviorConfig, Func<IConnection, TResult> func, Func<byte[]> keyFunc)
            {
                return _cluster.ExecuteCommand(BehaviorConfig, func, keyFunc);
            }
        }
    }
}