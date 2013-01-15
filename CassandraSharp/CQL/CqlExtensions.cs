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

namespace CassandraSharp.CQL
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.CQLBinaryProtocol;

    public static class CQLExtensions
    {
        public static Task<IList<T>> AsFuture<T>(this Task<IEnumerable<T>> @this)
        {
            return @this.ContinueWith(a => (IList<T>) a.Result.ToList());
        }

        public static Task ExecuteNonQuery(this ICluster cluster, string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            return CQLCommandHelpers.Query<Unit>(cluster, cql, cl, null).ContinueWith(res => res.Result.Count());
        }

        public static IPreparedQuery Prepare(this ICluster cluster, string cql)
        {
            return new PreparedQuery(cluster, cql);
        }
    }
}