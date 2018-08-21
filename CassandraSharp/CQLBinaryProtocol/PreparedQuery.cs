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

using System.Linq;
using CassandraSharp.CQLBinaryProtocol.Queries;
using CassandraSharp.Extensibility;

namespace CassandraSharp.CQLBinaryProtocol
{
    internal sealed class PreparedQuery<T> : IPreparedQuery<T>
    {
        private readonly ICluster _cluster;

        private readonly ConsistencyLevel? _consistencyLevel;

        private readonly string _cql;

        private readonly ExecutionFlags? _executionFlags;

        private readonly IDataMapperFactory _factoryIn;

        private readonly IDataMapperFactory _factoryOut;

        private readonly object _lock = new object();

        private IColumnSpec[] _columnSpecs;

        private volatile IConnection _connection;

        private byte[] _id;

        public PreparedQuery(ICluster cluster, ConsistencyLevel? consistencyLevel, ExecutionFlags? executionFlags,
                             IDataMapperFactory factoryIn,
                             IDataMapperFactory factoryOut, string cql)
        {
            _cluster = cluster;
            _consistencyLevel = consistencyLevel;
            _executionFlags = executionFlags;
            _factoryIn = factoryIn;
            _factoryOut = factoryOut;
            _cql = cql;
            _connection = null;
        }

        public IQuery<T> Execute(object dataSource)
        {
            ConsistencyLevel cl;
            ExecutionFlags executionFlags;
            IConnection connection;
            if (null == (connection = _connection))
                lock (_lock)
                {
                    if (null == (connection = _connection))
                    {
                        connection = _cluster.GetConnection();
                        connection.OnFailure += ConnectionOnOnFailure;

                        cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
                        executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;

                        var futPrepare = new PrepareQuery(connection, cl, executionFlags, _cql).AsFuture();
                        futPrepare.Wait();
                        var preparedInfo = futPrepare.Result.Single();

                        _id = preparedInfo.Item1;
                        _columnSpecs = preparedInfo.Item2;
                        _connection = connection;
                    }
                }

            cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
            executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;
            var mapperIn = _factoryIn.Create(dataSource.GetType());
            var mapperOut = _factoryOut.Create<T>();
            var futQuery = new ExecuteQuery<T>(connection, cl, executionFlags, _cql, _id, _columnSpecs, dataSource,
                                               mapperIn, mapperOut);
            return futQuery;
        }

        public void Dispose()
        {
            var connection = _connection;
            if (null != connection) connection.OnFailure -= ConnectionOnOnFailure;
        }

        private void ConnectionOnOnFailure(object sender, FailureEventArgs failureEventArgs)
        {
            var connection = _connection;
            if (null != connection) connection.OnFailure -= ConnectionOnOnFailure;

            _connection = null;
        }
    }
}