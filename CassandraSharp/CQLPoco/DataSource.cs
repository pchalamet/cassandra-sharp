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

namespace CassandraSharp.CQLPoco
{
    using System;
    using System.Reflection;
    using CassandraSharp.Extensibility;

    internal class DataSource : IDataSource
    {
        private readonly object _dataSource;

        public DataSource(object dataSource)
        {
            _dataSource = dataSource;
        }

        public object Get(IColumnSpec columnSpec)
        {
            const BindingFlags commonFlags = BindingFlags.Public | BindingFlags.IgnoreCase | BindingFlags.Instance;

            Type type = _dataSource.GetType();
            string name = columnSpec.Name;
            FieldInfo fieldInfo = type.GetField(name, commonFlags | BindingFlags.GetField);
            if (null != fieldInfo)
            {
                return fieldInfo.GetValue(_dataSource);
            }

            PropertyInfo propertyInfo = type.GetProperty(name, commonFlags | BindingFlags.SetProperty);
            if (null != propertyInfo)
            {
                return propertyInfo.GetValue(_dataSource, null);
            }

            string msg = string.Format("Can't find field or property {0}", columnSpec.Name);
            throw new ArgumentException(msg);
        }
    }
}