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
    using System.Threading;
    using System.Threading.Tasks;
    using CassandraSharp.Enlightenment;

    public static class CqlCommandExtensions
    {
        public static Task<IList<T>> AsFuture<T>(this IObservable<T> observable, CancellationToken? token = null)
        {
            return EnglightenmentMgr.Future().AsFuture(observable, token);
        }

        public static Task AsFuture(this IObservable<NonQuery> observable, CancellationToken? token = null)
        {
            return EnglightenmentMgr.Future().AsFuture(observable, token);
        }

        public static IQuery<NonQuery> Execute(this ICqlCommand @this, string cql)
        {
            return @this.Execute<NonQuery>(cql);
        }

        public static IPreparedQuery<NonQuery> Prepare(this ICqlCommand @this, string cql)
        {
            return @this.Prepare<NonQuery>(cql);
        }
    }
}