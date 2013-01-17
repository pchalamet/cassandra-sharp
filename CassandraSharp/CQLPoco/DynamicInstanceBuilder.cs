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

    internal class DynamicInstanceBuilder<T> : IInstanceBuilder where T : new()
    {
        private static readonly DynamicWriteAccessor<T> _accessor = new DynamicWriteAccessor<T>();

        private T _instance;

        public DynamicInstanceBuilder()
        {
            _instance = new T();
        }

        public void Set(IColumnSpec columnSpec, object data)
        {
            string colName = columnSpec.Name;
            try
            {
                _accessor.Set(ref _instance, colName, data);
            }
            catch (Exception)
            {
                if (colName.Contains("_"))
                {
                    colName = colName.Replace("_", "");
                }
                _accessor.Set(ref _instance, colName, data);
            }
        }

        public object Build()
        {
            return _instance;
        }
    }
}