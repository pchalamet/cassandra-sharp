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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Linq;
    using System.Reactive.Linq;
    using CassandraSharp.Extensibility;

    internal class PreparedQuery<T> : IPreparedQuery<T>
    {
        private readonly ICluster _cluster;

        private readonly string _cql;

        private readonly IDataMapper _dataMapper;

        private readonly ExecutionFlags _executionFlags;

        private readonly object _lock = new object();

        private IColumnSpec[] _columnSpecs;

        private volatile IConnection _connection;

        private byte[] _id;

        public PreparedQuery(ICluster cluster, IDataMapper dataMapper, string cql, ExecutionFlags executionFlags)
        {
            _cluster = cluster;
            _dataMapper = dataMapper;
            _cql = cql;
            _executionFlags = executionFlags;
        }

        public IObservable<T> Execute(object dataSource, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            IConnection connection;
            if (null == (connection = _connection))
            {
                lock (_lock)
                {
                    if (null == (connection = _connection))
                    {
                        connection = _cluster.GetConnection();
                        connection.OnFailure += ConnectionOnOnFailure;

                        var obsPrepare = CQLCommandHelpers.CreatePrepareQuery(connection, _cql, _executionFlags);
                        var futPrepare = obsPrepare.AsFuture();
                        futPrepare.Wait();
                        Tuple<byte[], IColumnSpec[]> preparedInfo = (Tuple<byte[], IColumnSpec[]>) futPrepare.Result.Single();

                        _id = preparedInfo.Item1;
                        _columnSpecs = preparedInfo.Item2;
                        _connection = connection;
                    }
                }
            }

            IDataMapperFactory factory = _dataMapper.Create<T>(dataSource);
            var query = CQLCommandHelpers.CreateExecuteQuery(connection, _id, _columnSpecs, cl, _executionFlags, _cql, factory);
            var queryT = query.Cast<T>();
            return queryT;
        }

        private void ConnectionOnOnFailure(object sender, FailureEventArgs failureEventArgs)
        {
            _connection = null;
        }
    }
}