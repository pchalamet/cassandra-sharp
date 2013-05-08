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

namespace CassandraSharp.CQLBinaryProtocol.Queries
{
    using System.IO;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils.Stream;

    internal sealed class ExecuteQuery<T> : CqlQuery<T>
    {
        private readonly IColumnSpec[] _columnSpecs;

        private readonly IDataMapper _mapperIn;

        private readonly byte[] _id;

        public ExecuteQuery(IConnection connection, string cql, byte[] id, IColumnSpec[] columnSpecs, IDataMapper mapperIn, IDataMapper mapperOut)
                : base(connection, cql, mapperOut)
        {
            _id = id;
            _columnSpecs = columnSpecs;
            _mapperIn = mapperIn;
        }

        protected override void WriteFrame(IFrameWriter fw)
        {
            Stream stream = fw.WriteOnlyStream;
            stream.WriteShortByteArray(_id);
            stream.WriteShort((short) _columnSpecs.Length);

            IDataSource dataSource = _mapperIn.DataSource;
            foreach (IColumnSpec columnSpec in _columnSpecs)
            {
                byte[] rawData = null;
                object data = dataSource.Get(columnSpec);
                if (null != data)
                {
                    rawData = columnSpec.Serialize(data);
                }

                stream.WriteByteArray(rawData);
            }

            stream.WriteShort((short) ConsistencyLevel);
            fw.SetMessageType(MessageOpcodes.Execute);
        }

        protected override InstrumentationToken CreateToken()
        {
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Prepare, ExecutionFlags, CQL);
            return token;
        }
    }
}