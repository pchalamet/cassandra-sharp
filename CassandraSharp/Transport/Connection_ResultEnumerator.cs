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
        //private IEnumerable<object> StreamResultsThenReleaseStreamId(Func<FrameReader, IEnumerable<object>> reader, byte streamId)
        //{
        //    // NOTE: this method runs inside a Task<IEnumerable<object>> but is an enumerator by itself
        //    //       this means that as soon as the task is running it will yield immediately
        //    //       rendering the task nearly immediately done.
        //    //       So this method should *not* be considered as running inside a task context
        //    try
        //    {
        //        _logger.Debug("Starting reading stream {0}@{1}", streamId, Endpoint);
        //        lock (_globalLock)
        //        {
        //            // release stream id (since result streaming has started)
        //            _availableStreamIds.Push(streamId);
        //            Monitor.Pulse(_globalLock);
        //        }

        //        // yield all rows - no lock required on input stream since we are the only one allowed to read
        //        using (FrameReader frameReader = FrameReader.ReadBody(_inputStream, _streaming))
        //        {
        //            // if no streaming we have read everything in memory
        //            // we can run a new reader immediately
        //            if (!_streaming)
        //            {
        //                Task.Factory.StartNew(ReadNextFrameHeader, _cancellation.Token);
        //            }

        //            foreach (object row in EnumerableOrEmptyEnumerable(reader(frameReader)))
        //            {
        //                yield return row;
        //            }
        //        }

        //        _logger.Debug("Done reading stream {0}@{1}", streamId, Endpoint);
        //        yield break;
        //    }
        //    finally
        //    {
        //        // run a new reader after streaming data
        //        if (_streaming)
        //        {
        //            Task.Factory.StartNew(ReadNextFrameHeader, _cancellation.Token);
        //        }
        //    }
        //}

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