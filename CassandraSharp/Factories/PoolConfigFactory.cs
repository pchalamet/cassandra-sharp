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

namespace CassandraSharp.Factories
{
    using System;
    using CassandraSharp.Config;
    using CassandraSharp.Pool;

    internal static class PoolConfigFactory
    {
        public static IPool<IConnection> Create(this PoolType @this, int poolSize)
        {
            switch (@this)
            {
                case PoolType.Stack:
                    return new StackPool<IConnection>(poolSize);

                case PoolType.Void:
                    return new VoidPool<IConnection>();
            }

            string msg = string.Format("Unknown connection pool type '{0}'", @this);
            throw new ArgumentException(msg);
        }
    }
}