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

namespace CassandraSharp.Transport
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal partial class Connection
    {
        internal class ResultStreamEnumerable : IEnumerable<object>
        {
            private readonly IEnumerator<object> _resultEnumerator;

            public ResultStreamEnumerable(Connection connection, Func<IFrameReader, IEnumerable<object>> reader, IFrameReader frameReader, byte streamId)
            {
                // prepare to stream the results
                IEnumerable<object> results = reader(frameReader);
                results = results ?? Enumerable.Empty<object>();
                IEnumerator<object> resultEnumerator = results.GetEnumerator();
                _resultEnumerator = new ResultStreamEnumerator(connection, frameReader, streamId, resultEnumerator);
            }

            public IEnumerator<object> GetEnumerator()
            {
                return _resultEnumerator;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        internal class ResultStreamEnumerator : IEnumerator<object>
        {
            private readonly Connection _connection;

            private readonly IFrameReader _frameReader;

            private readonly IEnumerator<object> _results;

            private readonly byte _streamId;

            private Exception _exception;

            public ResultStreamEnumerator(Connection connection, IFrameReader frameReader, byte streamId, IEnumerator<object> results)
            {
                _connection = connection;
                _frameReader = frameReader;
                _streamId = streamId;
                _results = results;
            }

            public bool MoveNext()
            {
                try
                {
                    return _results.MoveNext();
                }
                catch (Exception ex)
                {
                    _exception = ex;
                    throw;
                }
            }

            public void Reset()
            {
                try
                {
                    _results.Reset();
                }
                catch (Exception ex)
                {
                    _exception = ex;
                    throw;
                }
            }

            public object Current
            {
                get
                {
                    try
                    {
                        return _results.Current;
                    }
                    catch (Exception ex)
                    {
                        _exception = ex;
                        throw;
                    }
                }
            }

            object IEnumerator.Current
            {
                get { return Current; }
            }

            public void Dispose()
            {
                _connection.HandleFrameTerminaison(_exception, _frameReader, _streamId);
                _results.SafeDispose();
            }
        }
    }
}