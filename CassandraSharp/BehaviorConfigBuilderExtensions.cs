// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace CassandraSharp
{
    using System;
    using Apache.Cassandra;

    public static class BehaviorConfigBuilderExtensions
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