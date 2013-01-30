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

namespace CassandraSharp.CQLPropertyBag
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;

    public static class CQLPropertyBagExtensions
    {
        public static Task<IEnumerable<IDictionary<string, object>>> Execute(this ICluster cluster, string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            IDataMapperFactory factory = new DataMapperFactory();
            return CQLCommandHelpers.Query<IDictionary<string, object>>(cluster, cql, cl, factory, executionFlags);
        }

        public static Task<IEnumerable<T>> Execute<T>(this IPreparedQuery preparedQuery, IDictionary<string, object> dataSource, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            IDataMapperFactory factory = new DataMapperFactory(dataSource);
            return preparedQuery.Execute(cl, factory).ContinueWith(res => res.Result.Cast<T>());
        }

        public static Task<int> ExecuteNonQuery(this IPreparedQuery preparedQuery, IDictionary<string, object> dataSource, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            IDataMapperFactory factory = new DataMapperFactory(dataSource);
            return preparedQuery.Execute(cl, factory).ContinueWith(res => res.Result.Count());
        }
    }
}