// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
// 
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
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using CassandraSharp.Utils;

    internal class StackPool<T, E> : IPool<T, E> where T : IComparable<T>, IEquatable<T>
                                                 where E : IDisposable
    {
        private readonly Stack<E> _entries;

        private readonly int _poolSize;

        public StackPool(int poolSize)
        {
            _poolSize = poolSize;
            _entries = new Stack<E>();
        }

        public void Dispose()
        {
            foreach (E entry in _entries)
            {
                entry.SafeDispose();
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool Acquire(T token, out E entry)
        {
            if (0 < _entries.Count)
            {
                entry = _entries.Pop();
                return true;
            }

            entry = default(E);
            return false;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Release(T token, E entry)
        {
            int count = _entries.Count;
            if (count < _poolSize)
            {
                _entries.Push(entry);
            }
            else
            {
                entry.SafeDispose();
            }
        }
    }
}