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

namespace CassandraSharp
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using CassandraSharp.Enlightenment;

    public static class CqlCommandExtensions
    {
        public static Task<IList<T>> AsFuture<T>(this IObservable<T> observable)
        {
            return EnglightenmentMgr.Future().AsFuture(observable);
        }

        public static Task AsFuture(this IObservable<NonQuery> observable)
        {
            return EnglightenmentMgr.Future().AsFuture(observable);
        }

        [Obsolete("Use Execute(string)")]
        public static ICqlQuery<NonQuery> Execute(this ICqlCommand @this, string cql, ConsistencyLevel cl,
                                                  ExecutionFlags executionFlags = ExecutionFlags.None, object key = null)
        {
            return @this.Execute<NonQuery>(cql).WithConsistencyLevel(cl).WithExecutionFlags(executionFlags);
        }

        public static ICqlQuery<NonQuery> Execute(this ICqlCommand @this, string cql)
        {
            return @this.Execute<NonQuery>(cql);
        }

        public static IPreparedQuery<NonQuery> Prepare(this ICqlCommand @this, string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return @this.Prepare<NonQuery>(cql, executionFlags);
        }
    }
}