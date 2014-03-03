// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2014 Pierre Chalamet
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
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;

    internal sealed class DataMapper<T> : IDataMapper
    {
        private static readonly ClassMap<T> _classMap = ClassMap.GetClassMap<T>();

        public IEnumerable<IColumnData> MapToColumns(object dataSource, IEnumerable<IColumnSpec> columns)
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

        public object MapToObject(IEnumerable<IColumnData> rowData)
        {
            var instance = _classMap.CreateNewInstance();

            foreach (var column in rowData)
            {
                string colName = column.ColumnSpec.Name;

                var member = _classMap.GetMember(colName) ??
                             _classMap.GetMember(colName.Replace("_", string.Empty));

                if (member == null)
                {
                    continue;
                }

                var data = column.RawData != null
                        ? member.ValueSerializer.Deserialize(column.RawData)
                        : null;

                member.SetValue(instance, data);
            }

            var deserializationCallback = instance as IDeserializationCallback;
            if (null != deserializationCallback)
            {
                deserializationCallback.OnDeserialization(null);
            }

            return instance;
        }
    }
}