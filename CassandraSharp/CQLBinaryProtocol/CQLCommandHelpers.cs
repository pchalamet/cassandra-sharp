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
    using System;
    using System.Collections.Generic;
    using System.Security.Authentication;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;

    internal static class CQLCommandHelpers
    {
        //public static Task<IEnumerable<T>> Query<T>(ICluster cluster, string cql, ConsistencyLevel cl, IDataMapperFactory factory, ExecutionFlags executionFlags)
        //{
        //    IConnection connection = cluster.GetConnection(null);
        //    return Query(connection, cql, cl, factory, executionFlags).ContinueWith(res => res.Result.Cast<T>());
        //}

        internal static IQuery Query(IConnection connection, string cql, ConsistencyLevel cl, IDataMapperFactory factory,
                                     ExecutionFlags executionFlags)
        {
            Action<IFrameWriter> writer = fw => WriteQueryRequest(fw, cql, cl, MessageOpcodes.Query);
            Func<IFrameReader, IEnumerable<object>> reader = fr => ReadRowSet(fr, factory);

            InstrumentationToken token = InstrumentationToken.Create(RequestType.Query, executionFlags, cql);
            return connection.Execute(writer, reader, executionFlags, token);
        }

    }
}