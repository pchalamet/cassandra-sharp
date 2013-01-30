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
    using CassandraSharp.Utils;

    internal partial class Connection
    {
        internal class ResultStreamEnumerable : IEnumerable<object>
        {
            private readonly Connection _connection;

            private readonly Func<FrameReader, IEnumerable<object>> _reader;

            private readonly IEnumerator<object> _resultEnumerator;

            private readonly byte _streamId;

            public ResultStreamEnumerable(Connection connection, Func<FrameReader, IEnumerable<object>> reader, byte streamId)
            {
                FrameReader frameReader = null;
                try
                {
                    _connection = connection;
                    _reader = reader;
                    _streamId = streamId;

                    frameReader = _connection.ReleaseStreamId(_streamId);
                    IEnumerable<object> results = _reader(frameReader);
                    results = results ?? Enumerable.Empty<object>();
                    IEnumerator<object> resultEnumerator = results.GetEnumerator();
                    _resultEnumerator = new ResultStreamEnumerator(connection, frameReader, resultEnumerator);
                }
                catch (Exception ex)
                {
                    frameReader.SafeDispose();
                    connection.CompleteStreamRead();
                    connection.HandleFailure(ex);
                    throw;
                }
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

            private readonly FrameReader _frameReader;

            private readonly IEnumerator<object> _results;

            public ResultStreamEnumerator(Connection connection, FrameReader frameReader, IEnumerator<object> results)
            {
                _connection = connection;
                _frameReader = frameReader;
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
                    _connection.HandleFailure(ex);
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
                    _connection.HandleFailure(ex);
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
                        _connection.HandleFailure(ex);
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
                _frameReader.SafeDispose();
                _results.SafeDispose();
                _connection.CompleteStreamRead();
            }
        }
    }
}