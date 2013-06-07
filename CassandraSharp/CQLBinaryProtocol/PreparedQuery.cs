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
    using CassandraSharp.Utils;

    internal sealed class PreparedQuery<T> : IPreparedQuery<T>
    {
        private readonly ICluster _cluster;

        private readonly WeakReference<IConnection> _connection;

        private readonly ConsistencyLevel? _consistencyLevel;

        private readonly string _cql;

        private readonly ExecutionFlags? _executionFlags;

        private readonly IDataMapperFactory _factoryIn;

        private readonly IDataMapperFactory _factoryOut;

        private readonly object _lock = new object();

        private IColumnSpec[] _columnSpecs;

        private byte[] _id;

        public PreparedQuery(ICluster cluster, ConsistencyLevel? consistencyLevel, ExecutionFlags? executionFlags, IDataMapperFactory factoryIn,
                             IDataMapperFactory factoryOut, string cql)
        {
            _cluster = cluster;
            _consistencyLevel = consistencyLevel;
            _executionFlags = executionFlags;
            _factoryIn = factoryIn;
            _factoryOut = factoryOut;
            _cql = cql;
            _connection = new WeakReference<IConnection>(null);
        }

        public IQuery<T> Execute(object dataSource)
        {
            ConsistencyLevel cl;
            ExecutionFlags executionFlags;
            IConnection connection;
            if (!_connection.TryGetTarget(out connection))
            {
                lock (_lock)
                {
                    if (!_connection.TryGetTarget(out connection))
                    {
                        connection = _cluster.GetConnection();
                        connection.OnFailure += ConnectionOnOnFailure;

                        cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
                        executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;

                        var futPrepare = new PrepareQuery(connection, cl, executionFlags, _cql).AsFuture();
                        futPrepare.Wait();
                        Tuple<byte[], IColumnSpec[]> preparedInfo = futPrepare.Result.Single();

                        _id = preparedInfo.Item1;
                        _columnSpecs = preparedInfo.Item2;
                        _connection.SetTarget(connection);
                    }
                }
            }

            cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
            executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;
            IDataMapper mapperIn = _factoryIn.Create<T>(dataSource);
            IDataMapper mapperOut = _factoryOut.Create<T>();
            var futQuery = new ExecuteQuery<T>(connection, cl, executionFlags, _cql, _id, _columnSpecs, mapperIn, mapperOut);
            return futQuery;
        }

        private void ConnectionOnOnFailure(object sender, FailureEventArgs failureEventArgs)
        {
            _connection.SetTarget(null);
        }
    }
}