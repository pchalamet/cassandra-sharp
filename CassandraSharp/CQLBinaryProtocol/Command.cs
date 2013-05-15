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

    internal sealed class Command : ICqlCommand
    {
        private readonly ICluster _cluster;

        private readonly IDataMapperFactory _factoryIn;

        private readonly IDataMapperFactory _factoryOut;

        public Command(ICluster cluster, IDataMapperFactory factoryIn, IDataMapperFactory factoryOut)
        {
            _cluster = cluster;
            _factoryIn = factoryIn;
            _factoryOut = factoryOut;
        }

        public ICqlQuery<T> Execute<T>(string cql, object dataSource)
        {
            if (null != dataSource)
            {
                throw new ArgumentException("Binary protocol v2 is not implemented");
            }

            IDataMapper factoryOut = _factoryOut.Create<T>();
            IConnection connection = _cluster.GetConnection();
            ICqlQuery<T> query = new CqlQuery<T>(connection, cql, factoryOut);
            return query;
        }

        public IPreparedQuery<T> Prepare<T>(string cql, ExecutionFlags executionFlags)
        {
            return new PreparedQuery<T>(_cluster, _factoryIn, _factoryOut, cql, executionFlags);
        }
    }
}