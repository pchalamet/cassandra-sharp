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

namespace CassandraSharp.CQLPoco
{
    using System.Runtime.Serialization;
    using CassandraSharp.Extensibility;
    using System.Collections;
    using System.Collections.Generic;
    using CassandraSharp.Utils.Stream;
    using CassandraSharp.CQLBinaryProtocol;
    using System;
    using CassandraSharp.Exceptions;

    internal sealed class InstanceBuilder<T> : IInstanceBuilder
    {
        private static readonly ClassMap<T> _classMap = ClassMap.GetClassMap<T>();

        private T _instance;

        public InstanceBuilder()
        {
            _instance = _classMap.CreateNewInstance();
        }

        public bool SetRaw(IColumnSpec columnSpec, byte[] rawData)
        {
            if (rawData != null)
            {
                var data = columnSpec.Deserialize(rawData);
                Set(columnSpec, data);
            }

            return false;
        }

        public bool Set(IColumnSpec columnSpec, object data)
        {
            string colName = columnSpec.Name;

            var member = _classMap.GetMember(colName) ??
                         _classMap.GetMember(colName.Replace("_", string.Empty));

            if (member == null)
            {
                return false;
            }
            
            member.SetValue(_instance, data);

            return true;
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

        public object BuildObjectInstance(IEnumerable<KeyValuePair<IColumnSpec, byte[]>> rowData)
        {
            _instance = _classMap.CreateNewInstance();

            foreach (var column in rowData)
            {                
                string colName = column.Key.Name;

                var member = _classMap.GetMember(colName) ??
                             _classMap.GetMember(colName.Replace("_", string.Empty));

                if (member == null)
                {
                    throw new DataMappingException(string.Format("Object doesn't have specified column: {0}", colName));
                }

                var data = member.ValueSerializer.Deserialize(column.Value);
                member.SetValue(_instance, data);
            }

            return Build();
        }
    }
}