// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using CassandraSharp.Extensibility;

namespace CassandraSharp.CQLBinaryProtocol
{
    internal sealed class ColumnSpec : IColumnSpec
    {
        public ColumnSpec(int index, string keyspace, string table, string name, ColumnType columnType,
                          string customData, ColumnType collectionKeyType,
                          ColumnType collectionValueType)
        {
            Index = index;
            Keyspace = keyspace;
            Table = table;
            Name = name;
            ColumnType = columnType;
            CustomData = customData;
            CollectionKeyType = collectionKeyType;
            CollectionValueType = collectionValueType;
        }

        public int Index { get; }

        public string Keyspace { get; }

        public string Table { get; }

        public string Name { get; }

        public ColumnType ColumnType { get; }

        public string CustomData { get; }

        public ColumnType CollectionKeyType { get; }

        public ColumnType CollectionValueType { get; }
    }
}