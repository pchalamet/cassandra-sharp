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
    using System.Reflection;
    using System.Runtime.Serialization;
    using CassandraSharp.Extensibility;

    internal class InstanceBuilder : IInstanceBuilder
    {
        private readonly object _instance;

        private readonly Type _type;

        public InstanceBuilder(Type type)
        {
            _type = type;
            _instance = Activator.CreateInstance(_type);
        }

        public bool Set(IColumnSpec columnSpec, object data)
        {
            if (TrySet(columnSpec.Name, data))
            {
                return true;
            }

            if (columnSpec.Name.Contains("_"))
            {
                string newName = columnSpec.Name.Replace("_", "");
                return TrySet(newName, data);
            }

            return false;
        }

        public object Build()
        {
            IDeserializationCallback cb = _instance as IDeserializationCallback;
            if (null != cb)
            {
                cb.OnDeserialization(null);
            }

            return _instance;
        }

        private bool TrySet(string name, object data)
        {
            const BindingFlags commonFlags = BindingFlags.Public | BindingFlags.IgnoreCase | BindingFlags.Instance;

            FieldInfo fieldInfo = _type.GetField(name, commonFlags | BindingFlags.SetField);
            if (null != fieldInfo)
            {
                fieldInfo.SetValue(_instance, data);
                return true;
            }

            PropertyInfo propertyInfo = _type.GetProperty(name, commonFlags | BindingFlags.SetProperty);
            if (null != propertyInfo)
            {
                propertyInfo.SetValue(_instance, data, null);
                return true;
            }

            return false;
        }
    }
}