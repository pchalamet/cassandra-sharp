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
    using System.Numerics;
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.Extensibility;

    internal sealed class CqlCommand : ICqlCommand
    {
        private readonly ICluster _cluster;

        private readonly IDataMapperFactory _factoryIn;

        private readonly IDataMapperFactory _factoryOut;

        private ConsistencyLevel? _consistencyLevel;

        private ExecutionFlags? _executionFlags;

        public CqlCommand(ICluster cluster, IDataMapperFactory factoryIn, IDataMapperFactory factoryOut)
        {
            _cluster = cluster;
            _factoryIn = factoryIn;
            _factoryOut = factoryOut;
        }

        public ICqlCommand WithConsistencyLevel(ConsistencyLevel cl)
        {
            _consistencyLevel = cl;
            return this;
        }

        public ICqlCommand WithExecutionFlags(ExecutionFlags executionFlags)
        {
            _executionFlags = executionFlags;
            return this;
        }

        public IQuery<T> Execute<T>(string cql, object dataSource, PartitionKey partitionKey)
        {
            if (null != dataSource)
            {
                throw new ArgumentException("Binary protocol v2 is not implemented");
            }

            // grab a connection
            BigInteger? token = partitionKey == null ? null : _cluster.Partitioner.ComputeToken(partitionKey);
            IConnection connection = _cluster.GetConnection(token);

            // define execution context
            ConsistencyLevel cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
            ExecutionFlags executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;

            // out to spit out results
            IDataMapper factoryOut = _factoryOut.Create<T>();

            IQuery<T> query = new CqlQuery<T>(connection, cl, executionFlags, cql, factoryOut);
            return query;
        }

        public IPreparedQuery<T> Prepare<T>(string cql)
        {
            return new PreparedQuery<T>(_cluster, _consistencyLevel, _executionFlags, _factoryIn, _factoryOut, cql);
        }
    }
}