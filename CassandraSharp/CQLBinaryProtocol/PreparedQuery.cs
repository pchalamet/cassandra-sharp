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
    using CassandraSharp.CQLBinaryProtocol.Queries;
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

        [Obsolete("Use Execute(object) instead")]
        public ICqlQuery<T> Execute(object dataSource, ConsistencyLevel cl, QueryHint hint = null)
        {
            return Execute(dataSource).WithConsistencyLevel(cl).WithExecutionFlags(_executionFlags).WithHint(hint);
        }

        public ICqlQuery<T> Execute(object dataSource)
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

                        var obsPrepare = new PrepareQuery(connection, _cql);
                        var futPrepare = obsPrepare.AsFuture();
                        futPrepare.Wait();
                        Tuple<byte[], IColumnSpec[]> preparedInfo = futPrepare.Result.Single();

                        _id = preparedInfo.Item1;
                        _columnSpecs = preparedInfo.Item2;
                        _connection = connection;
                    }
                }
            }

            IDataMapperFactory factory = _dataMapper.Create<T>(dataSource);
            var query = new ExecuteQuery<T>(connection, _cql, _id, _columnSpecs, factory).WithExecutionFlags(_executionFlags);
            return query;
        }

        private void ConnectionOnOnFailure(object sender, FailureEventArgs failureEventArgs)
        {
            _connection = null;
        }
    }
}