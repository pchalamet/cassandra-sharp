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