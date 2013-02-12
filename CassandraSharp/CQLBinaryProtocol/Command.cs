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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.CQL;
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

        public Task<IEnumerable<T>> Execute<T>(string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM,
                                               ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            IDataMapperFactory factory = DataMapper.Create<T>();
            return CQLCommandHelpers.Query<T>(Cluster, cql, cl, factory, executionFlags);
        }

        public Task Execute(string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return CQLCommandHelpers.Query<Unit>(Cluster, cql, cl, null, executionFlags).ContinueWith(res => res.Result.Count());
        }

        public IPreparedQuery<T> Prepare<T>(string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return new PreparedQuery<T>(Cluster, DataMapper, cql, executionFlags);
        }

        public IPreparedQuery Prepare(string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return new PreparedNonQuery(Cluster, DataMapper, cql, executionFlags);
        }
    }
}