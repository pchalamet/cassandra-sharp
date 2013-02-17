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

namespace CassandraSharp.CQL
{
    using System;
    using System.Collections.Generic;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Threading.Tasks;

    public static class CQLExtensions
    {
        public static Task<IList<T>> AsFuture<T>(this IObservable<T> observable)
        {
            var obsEnumerable = observable.Aggregate((IList<T>) new List<T>(),
                                                     (acc, v) =>
                                                         {
                                                             acc.Add(v);
                                                             return acc;
                                                         });
            var task = obsEnumerable.ToTask();
            return task;
        }

        public static Task AsFuture(this IObservable<NonQuery> observable)
        {
            Task task = observable.Count().ToTask();
            return task;
        }
    }
}