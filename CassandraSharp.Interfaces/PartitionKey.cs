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

namespace CassandraSharp
{
    using System;

    public class PartitionKey
    {
        private PartitionKey(object[] keys)
        {
            Keys = keys;
        }

        public object[] Keys { get; set; }

        public static PartitionKey From(params object[] keys)
        {
            if (null == keys || 0 == keys.Length)
            {
                throw new ArgumentException("Keys array must contain at least one element", "keys");
            }

            return new PartitionKey(keys);
        }
    }
}