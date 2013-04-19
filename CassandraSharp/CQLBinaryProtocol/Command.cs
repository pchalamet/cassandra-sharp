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
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.Extensibility;

    public abstract class Command : ICqlCommand
    {
        protected readonly ICluster Cluster;

        protected readonly IDataMapper DataMapper;

        protected Command(ICluster cluster, IDataMapper dataMapper)
        {
            Cluster = cluster;
            DataMapper = dataMapper;
        }

        [Obsolete("Use Execute(string) instead")]
        public ICqlQuery<T> Execute<T>(string cql, ConsistencyLevel cl, ExecutionFlags executionFlags = ExecutionFlags.None,
                                       QueryHint hint = null)
        {
            IDataMapperFactory factory = DataMapper.Create<T>();
            IConnection connection = Cluster.GetConnection();
            ICqlQuery<T> query = new CqlQuery<T>(connection, cql, factory).WithConsistencyLevel(cl).WithExecutionFlags(executionFlags);
            return query;
        }

        public ICqlQuery<T> Execute<T>(string cql)
        {
            IDataMapperFactory factory = DataMapper.Create<T>();
            IConnection connection = Cluster.GetConnection();
            ICqlQuery<T> query = new CqlQuery<T>(connection, cql, factory);
            return query;
        }

        public IPreparedQuery<T> Prepare<T>(string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return new PreparedQuery<T>(Cluster, DataMapper, cql, executionFlags);
        }
    }
}