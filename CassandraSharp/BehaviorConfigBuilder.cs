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
    using CassandraSharp.Config;

    public static class BehaviorConfigExtensions
    {
        public static IBehaviorConfig Override(this IBehaviorConfig @this, IBehaviorConfig behaviorConfigToOverride)
        {
            IBehaviorConfig behaviorConfigConfig = new BehaviorConfig
                                                       {
                                                           KeySpace = @this.KeySpace ?? behaviorConfigToOverride.KeySpace,
                                                           MaxRetries = @this.MaxRetries ?? behaviorConfigToOverride.MaxRetries,
                                                           ReadConsistencyLevel = @this.ReadConsistencyLevel ?? behaviorConfigToOverride.ReadConsistencyLevel,
                                                           WriteConsistencyLevel = @this.WriteConsistencyLevel ?? behaviorConfigToOverride.WriteConsistencyLevel,
                                                           TTL = @this.TTL ?? behaviorConfigToOverride.TTL,
                                                           RetryOnNotFound = @this.RetryOnNotFound ?? behaviorConfigToOverride.RetryOnNotFound,
                                                           RetryOnTimeout = @this.RetryOnTimeout ?? behaviorConfigToOverride.RetryOnTimeout,
                                                           RetryOnUnavailable = @this.RetryOnUnavailable ?? behaviorConfigToOverride.RetryOnUnavailable,
                                                           SleepBeforeRetry = @this.SleepBeforeRetry ?? behaviorConfigToOverride.SleepBeforeRetry
                                                       };

            return behaviorConfigConfig;
        }
    }
}