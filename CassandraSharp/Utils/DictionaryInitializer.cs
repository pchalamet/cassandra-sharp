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
    using System.Collections.Generic;

    internal sealed class DictionaryInitializer<K, V> : IDictionaryInitializer
    {
        private readonly Dictionary<K, V> _collection = new Dictionary<K, V>();

        public void Add(object key, object value)
        {
            K tkey = (K) key;
            V vvalue = (V) value;
            _collection.Add(tkey, vvalue);
        }

        public object Collection
        {
            get { return _collection; }
        }
    }
}