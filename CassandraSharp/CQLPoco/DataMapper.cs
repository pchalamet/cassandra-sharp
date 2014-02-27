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
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    internal sealed class DataMapper<T> : IDataMapper
    {
        private static readonly ClassMap<T> _classMap = ClassMap.GetClassMap<T>();

        public IEnumerable<IColumnData> GetColumnData(object dataSource, IEnumerable<IColumnSpec> columns)
        {            
            foreach (var column in columns)
            {
                string colName = column.Name;

                var member = _classMap.GetMember(colName) ??
                             _classMap.GetMember(colName.Replace("_", string.Empty));

                if (member == null)
                {
                    throw new DataMappingException(string.Format("Object doesn't have specified column: {0}", colName));
                }

                var value = member.GetValue(dataSource);
                byte[] rawData = null;
                if (value != null)
                {
                    rawData = member.ValueSerializer.Serialize(value);
                }

                yield return new ColumnData(column, rawData);
            }
        }

        public object BuildObjectInstance(IEnumerable<IColumnData> rowData)
        {
            var instance = _classMap.CreateNewInstance();

            foreach (var column in rowData)
            {
                string colName = column.ColumnSpec.Name;

                var member = _classMap.GetMember(colName) ??
                             _classMap.GetMember(colName.Replace("_", string.Empty));

                if (member == null)
                {
                    throw new DataMappingException(string.Format("Object doesn't have specified column: {0}", colName));
                }

                var data = column.RawData != null ?
                    member.ValueSerializer.Deserialize(column.RawData) :
                    null;

                member.SetValue(instance, data);
            }

            if (instance is IDeserializationCallback)
            {
                (instance as IDeserializationCallback).OnDeserialization(null);
            }

            return instance;
        }
    }
}