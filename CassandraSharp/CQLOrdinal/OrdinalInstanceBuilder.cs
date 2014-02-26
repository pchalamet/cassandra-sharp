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

namespace CassandraSharp.CQLOrdinal
{
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp.Extensibility;
    using CassandraSharp.CQLBinaryProtocol;

    internal sealed class OrdinalInstanceBuilder : IInstanceBuilder
    {
        private readonly SortedDictionary<int, object> _instance = new SortedDictionary<int, object>();

        public bool Set(IColumnSpec columnSpec, object data)
        {
            int idx = columnSpec.Index;
            _instance[idx] = data;
            return true;
        }

        public object Build()
        {
            // compute upper key
            var keys = _instance.Keys.ToArray();
            int len = 0;
            if (keys.Length > 0)
            {
                len = 1 + keys[keys.Length - 1];
            }

            // create destination container
            object[] instance = new object[len];
            foreach (var kvp in _instance)
            {
                instance[kvp.Key] = kvp.Value;
            }

            return instance;
        }

        public object BuildObjectInstance(IEnumerable<KeyValuePair<IColumnSpec, byte[]>> rowData)
        {
            foreach (var column in rowData)
            {
                var data = ValueSerialization.Deserialize(column.Key, column.Value);
                Set(column.Key, data);
            }

            return Build();
        }
    }
}