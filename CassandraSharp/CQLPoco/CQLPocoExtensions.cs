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

namespace CassandraSharp.CQLPoco
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public static class CQLPocoExtensions
    {
        public static ICqlCommand CreatePocoCommand(this ICluster @this)
        {
            return new PocoCommand(@this);
        }

        [Obsolete("Use PocoCommand.Execute<T>() instead")]
        public static Task<IEnumerable<T>> Execute<T>(this ICluster cluster, string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM,
                                                      ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePocoCommand();
            return cmd.Execute<T>(cql, cl, executionFlags);
        }

        [Obsolete("Use PocoCommand.Execute() instead")]
        public static Task ExecuteNonQuery(this ICluster cluster, string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM,
                                           ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePocoCommand();
            return cmd.Execute(cql, cl, executionFlags);
        }

        [Obsolete("Use PocoCommand.Prepare<T>() instead")]
        public static IPreparedQuery<T> Prepare<T>(this ICluster cluster, string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePocoCommand();
            IPreparedQuery<T> preparedQuery = cmd.Prepare<T>(cql, executionFlags);
            return preparedQuery;
        }

        [Obsolete("Use PocoCommand.Prepare() instead")]
        public static IPreparedQuery PrepareNonQuery(this ICluster cluster, string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePocoCommand();
            IPreparedQuery preparedQuery = cmd.Prepare(cql, executionFlags);
            return preparedQuery;
        }
    }
}