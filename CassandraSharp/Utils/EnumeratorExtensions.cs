// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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

namespace CassandraSharp.Utils
{
    using System.Collections;
    using System.Collections.Generic;

    internal static class EnumeratorExtensions
    {
        public static T Single<T>(this IEnumerator<T> enumerator)
        {
            enumerator.MoveNext();
            return enumerator.Current;
        }

        public static IList<T> ToList<T>(this IEnumerator<T> enumerator)
        {
            List<T> list = new List<T>();
            enumerator.Reset();
            while (enumerator.MoveNext())
            {
                list.Add(enumerator.Current);
            }

            return list;
        }

        public static int Count<T>(this IEnumerator<T> enumerator)
        {
            enumerator.Reset();
            int count = 0;
            while (enumerator.MoveNext())
            {
                ++count;
            }

            return count;
        }

        public static IEnumerator<T> Cast<T>(this IEnumerator<object> enumerator)
        {
            return new CastEnumerator<T>(enumerator);
        }

        private class CastEnumerator<T> : IEnumerator<T>
        {
            private readonly IEnumerator<object> _enumerator;

            public CastEnumerator(IEnumerator<object> enumerator)
            {
                _enumerator = enumerator;
            }

            public void Dispose()
            {
                _enumerator.Dispose();
            }

            public bool MoveNext()
            {
                return _enumerator.MoveNext();
            }

            public void Reset()
            {
                _enumerator.Reset();
            }

            public T Current
            {
                get { return (T) _enumerator.Current; }
            }

            object IEnumerator.Current
            {
                get { return Current; }
            }
        }
    }
}