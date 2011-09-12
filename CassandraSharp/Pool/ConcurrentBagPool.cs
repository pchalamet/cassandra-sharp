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

#if NET4
namespace CassandraSharp.Pool
{
    using System;
    using System.Collections.Concurrent;
    using CassandraSharp.Config;

    internal class BagPool<T> : IPool<T> where T : IDisposable
    {
        private readonly ConcurrentBag<T> _entries;

        private readonly int _max;

        public BagPool(PoolConfig config)
        {
            _max = config.Max;
            _entries = new ConcurrentBag<T>();
        }

        public bool Acquire(out T entry)
        {
            return _entries.TryTake(out entry);
        }

        public void Release(T entry)
        {
            int count = _entries.Count;
            if (count < _max)
            {
                _entries.Add(entry);
            }
            else
            {
                entry.SafeDispose();
            }
        }

        public void Dispose()
        {
            foreach (T entry in _entries)
            {
                entry.Dispose();
            }
        }
    }
}
#endif