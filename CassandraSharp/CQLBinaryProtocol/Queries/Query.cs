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

namespace CassandraSharp.CQLBinaryProtocol.Queries
{
    using System;
    using System.Collections.Generic;
    using System.Reactive.Disposables;
    using CassandraSharp.Extensibility;

    internal abstract class Query<T> : ICqlQuery<T>
    {
        private readonly IConnection _connection;

        protected Query(IConnection connection)
        {
            _connection = connection;
            ConsistencyLevel = ConsistencyLevel.QUORUM;
            ExecutionFlags = ExecutionFlags.None;
            QueryHint = null;
        }

        protected ConsistencyLevel ConsistencyLevel { get; private set; }

        protected ExecutionFlags ExecutionFlags { get; private set; }

        protected QueryHint QueryHint { get; private set; }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            var writer = CreateWriter();
            Func<IFrameReader, IEnumerable<T>> reader = CreateReader;
            var token = CreateToken();

            _connection.Execute(writer, reader, token, observer);
            return Disposable.Empty;
        }

        public ICqlQuery<T> WithConsistencyLevel(ConsistencyLevel cl)
        {
            ConsistencyLevel = cl;
            return this;
        }

        public ICqlQuery<T> WithExecutionFlags(ExecutionFlags executionFlags)
        {
            ExecutionFlags = executionFlags;
            return this;
        }

        public ICqlQuery<T> WithHint(QueryHint hint)
        {
            QueryHint = hint;
            return this;
        }

        protected abstract IEnumerable<T> CreateReader(IFrameReader frameReader);

        protected abstract Action<IFrameWriter> CreateWriter();

        protected abstract InstrumentationToken CreateToken();
    }
}