// cassandra-sharp - a .NET client for Apache Cassandra
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

namespace CassandraSharp.CQLPoco
{
    using System;
    using CassandraSharp.Extensibility;

    internal class DynamicDataSource<T> : IDataSource
    {
        private static readonly DynamicReadAccessor<T> _accessor = new DynamicReadAccessor<T>();

        private T _dataSource;

        internal T Datasource
        {
            get { return _dataSource; }
            set { _dataSource = value; }
        }

        internal DynamicDataSource()
        { 
        }

        public DynamicDataSource(T dataSource)
        {
            _dataSource = dataSource;
        }

        public object Get(IColumnSpec columnSpec)
        {
            string colName = columnSpec.Name;
            try
            {
                return _accessor.Get(ref _dataSource, colName);
            }
            catch (ArgumentException)
            {
                if (colName.Contains("_"))
                {
                    colName = colName.Replace("_", "");
                }

                return _accessor.Get(ref _dataSource, colName);
            }
        }
    }
}