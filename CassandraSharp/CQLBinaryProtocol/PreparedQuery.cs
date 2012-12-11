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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using CassandraSharp.Extensibility;

    internal class PreparedQuery : IPreparedQuery
    {
        private readonly IColumnSpec[] _columnSpecs;

        private readonly IConnection _connection;

        private readonly byte[] _id;

        public PreparedQuery(IConnection connection, byte[] id, IColumnSpec[] columnSpecs)
        {
            _connection = connection;
            _id = id;
            _columnSpecs = columnSpecs;
        }

        public Task<IEnumerable<object>> Execute(ConsistencyLevel cl, IDataMapperFactory factory)
        {
            Action<IFrameWriter> writer = fw => WriteExecuteRequest(fw, cl, factory);
            Func<IFrameReader, IEnumerable<object>> reader = fr => CQLCommandHelpers.ReadRowSet(fr, factory);

            return _connection.Execute(writer, reader);
        }

        private void WriteExecuteRequest(IFrameWriter frameWriter, ConsistencyLevel cl, IDataMapperFactory factory)
        {
            frameWriter.WriteShortByteArray(_id);
            frameWriter.WriteShort((short) _columnSpecs.Length);

            IDataSource dataSource = factory.DataSource;
            foreach (IColumnSpec columnSpec in _columnSpecs)
            {
                object data = dataSource.Get(columnSpec);
                byte[] rawData = columnSpec.Serialize(data);
                frameWriter.WriteByteArray(rawData);
            }

            frameWriter.WriteShort((short) cl);
            frameWriter.Send(MessageOpcodes.Execute);
        }
    }
}