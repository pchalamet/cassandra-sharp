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

namespace CassandraSharp.Enlightenment
{
    using System;
    using System.Collections.Generic;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Threading;
    using System.Threading.Tasks;

    internal sealed class Future : IFuture
    {
        public Task<IList<T>> AsFuture<T>(IObservable<T> observable, CancellationToken? token)
        {
            var obsEnumerable = observable.Aggregate((IList<T>)new List<T>(),
                                                     (acc, v) =>
                                                         {
                                                             acc.Add(v);
                                                             return acc;
                                                         });

            Task<IList<T>> task = token.HasValue
                                          ? obsEnumerable.ToTask(token.Value)
                                          : obsEnumerable.ToTask();
            return task;
        }

        public Task AsFuture(IObservable<NonQuery> observable, CancellationToken? token)
        {
            var obsEnumerable = observable.Count();
            Task task = token.HasValue
                                ? obsEnumerable.ToTask(token.Value)
                                : obsEnumerable.ToTask();
            return task;
        }
    }
}