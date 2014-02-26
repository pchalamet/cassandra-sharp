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

namespace CassandraSharp.CQLPropertyBag
{
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;
    using System;
    using System.Collections.Generic;

    internal sealed class DataSource : IDataSource
    {
        private readonly PropertyBag _dataSource;

        public DataSource(PropertyBag dataSource)
        {
            _dataSource = dataSource;
        }

        public object Get(IColumnSpec columnSpec)
        {
            return _dataSource[columnSpec.Name];
        }

        public IEnumerable<byte[]> GetColumnData(IEnumerable<IColumnSpec> columns)
        {
            foreach (var column in columns)
            {
                var value = Get(column);
                if (value == null)
                {                    
                    yield return null;
                    continue;
                }
                
                yield return ValueSerialization.Serialize(column, value);
            }
        }
    }
}