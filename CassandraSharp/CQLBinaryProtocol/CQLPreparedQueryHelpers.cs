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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;

    internal class CQLPreparedQueryHelpers
    {
        private readonly ICluster _cluster;

        private readonly string _cql;

        private readonly ExecutionFlags _executionFlags;

        private readonly object _lock = new object();

        private IColumnSpec[] _columnSpecs;

        private volatile IConnection _connection;

        private byte[] _id;

        public CQLPreparedQueryHelpers(ICluster cluster, string cql, ExecutionFlags executionFlags)
        {
            _cluster = cluster;
            _cql = cql;
            _executionFlags = executionFlags;
        }

        public IQuery Execute(IDataMapperFactory factory, ConsistencyLevel cl, Action<IEnumerable<object> dataAvailable)
        {
            IConnection connection;
            if (null == (connection = _connection))
            {
                lock (_lock)
                {
                    if (null == (connection = _connection))
                    {
                        connection = _cluster.GetConnection(null);
                        connection.OnFailure += ConnectionOnOnFailure;

                        Action<IFrameWriter> writer = fw => CQLCommandHelpers.WritePrepareRequest(fw, _cql);
                        Func<IFrameReader, IEnumerable<object>> reader = fr => CQLCommandHelpers.ReadPreparedQuery(fr, connection);

                        InstrumentationToken prepareToken = InstrumentationToken.Create(RequestType.Prepare, _executionFlags, _cql);
                        connection.Execute(writer, reader, _executionFlags, prepareToken).ContinueWith(ReadPreparedQueryInfo).Wait();
                        _connection = connection;
                    }
                }
            }

            Action<IFrameWriter> execWriter = fw => WriteExecuteRequest(fw, cl, factory);
            Func<IFrameReader, IEnumerable<object>> execReader = fr => CQLCommandHelpers.ReadRowSet(fr, factory);

            InstrumentationToken queryToken = InstrumentationToken.Create(RequestType.Query, _executionFlags, _cql);
            return connection.Execute(execWriter, execReader, _executionFlags, queryToken);
        }

        private void ConnectionOnOnFailure(object sender, FailureEventArgs failureEventArgs)
        {
            _connection = null;
        }

        private void ReadPreparedQueryInfo(Task<IEnumerable<object>> results)
        {
            Tuple<byte[], IColumnSpec[]> preparedInfo = (Tuple<byte[], IColumnSpec[]>) results.Result.Single();
            _id = preparedInfo.Item1;
            _columnSpecs = preparedInfo.Item2;
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
            frameWriter.SetMessageType(MessageOpcodes.Execute);
        }
    }
}