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
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public static class CQLPropertyBagExtensions
    {
        public static ICqlCommand CreatePropertyBagCommand(this ICluster @this)
        {
            return new PropertyBagCommand(@this);
        }

        [Obsolete("Use PropertyBagCommand.Execute<T>() instead")]
        public static Task<IEnumerable<IDictionary<string, object>>> Execute(this ICluster cluster, string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM,
                                                                             ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePropertyBagCommand();
            return cmd.Execute<IDictionary<string, object>>(cql, cl, executionFlags);
        }

        [Obsolete("Use PropertyBagCommand.Execute() instead")]
        public static Task ExecuteNonQuery(this ICluster cluster, string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM,
                                           ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePropertyBagCommand();
            return cmd.Execute(cql, cl, executionFlags);
        }

        [Obsolete("Use PropertyBagCommand.Prepare<T>() instead")]
        public static IPreparedQuery<IDictionary<string, object>> Prepare(this ICluster cluster, string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePropertyBagCommand();
            IPreparedQuery<IDictionary<string, object>> preparedQuery = cmd.Prepare<IDictionary<string, object>>(cql, executionFlags);
            return preparedQuery;
        }

        [Obsolete("Use PropertyBagCommand.Prepare() instead")]
        public static IPreparedQuery PrepareNonQuery(this ICluster cluster, string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            var cmd = cluster.CreatePropertyBagCommand();
            IPreparedQuery preparedQuery = cmd.Prepare(cql, executionFlags);
            return preparedQuery;
        }
    }
}