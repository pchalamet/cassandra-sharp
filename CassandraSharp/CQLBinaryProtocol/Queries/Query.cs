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

using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using CassandraSharp.Extensibility;

namespace CassandraSharp.CQLBinaryProtocol.Queries
{
    internal abstract class Query<T> : IQuery<T>
    {
        private readonly IConnection _connection;

        protected Query(IConnection connection, ConsistencyLevel consistencyLevel, ExecutionFlags executionFlags)
        {
            _connection = connection;
            ConsistencyLevel = consistencyLevel;
            ExecutionFlags = executionFlags;
        }

        protected ConsistencyLevel ConsistencyLevel { get; private set; }

        protected ExecutionFlags ExecutionFlags { get; private set; }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            Action<IFrameWriter> writer = WriteFrame;
            Func<IFrameReader, IEnumerable<T>> reader = ReadFrame;
            var token = CreateInstrumentationToken();

            _connection.Execute(writer, reader, token, observer);
            return Disposable.Empty;
        }

        protected abstract IEnumerable<T> ReadFrame(IFrameReader frameReader);

        protected abstract void WriteFrame(IFrameWriter frameWriter);

        protected abstract InstrumentationToken CreateInstrumentationToken();
    }
}