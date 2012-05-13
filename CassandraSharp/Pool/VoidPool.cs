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

namespace CassandraSharp.Pool
{
    using System;
    using CassandraSharp.Utils;

    internal class VoidPool<T> : IPool<T> where T : IDisposable
    {
        public void Dispose()
        {
        }

        public bool Acquire(out T entry)
        {
            entry = default(T);
            return false;
        }

        public void Release(T entry)
        {
            entry.SafeDispose();
        }
    }
}