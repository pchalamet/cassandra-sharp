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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Collections.Generic;
    using System.Reactive.Disposables;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;

    public class Query : IObservable<object>
    {
        private readonly IConnection _connection;

        private readonly Func<IFrameReader, IEnumerable<object>> _reader;

        private readonly InstrumentationToken _token;

        private readonly Action<IFrameWriter> _writer;

        public Query(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader, InstrumentationToken token, IConnection connection)
        {
            _writer = writer;
            _reader = reader;
            _token = token;
            _connection = connection;
        }

        public IDisposable Subscribe(IObserver<object> observer)
        {
            _connection.Execute(_writer, _reader, _token, observer);
            return Disposable.Empty;
        }
    }
}