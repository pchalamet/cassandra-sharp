// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Security.Authentication;
    using System.Threading;
    using System.Threading.Tasks;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Config;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal class Connection : IConnection
    {
// ReSharper disable InconsistentNaming
        private const byte STREAMID_MAX = 0x80;

// ReSharper restore InconsistentNaming

        private readonly Stack<byte> _availableStreamIds = new Stack<byte>(STREAMID_MAX);

        private readonly TransportConfig _config;

        private readonly object _globalLock = new object();

        private readonly Stream _inputStream;

        private readonly Stream _outputStream;

        private readonly RequestState[] _requestStates = new RequestState[STREAMID_MAX];

        private readonly TcpClient _tcpClient;

        private Task _currReadTask;

        private bool _failed;

        public Connection(IPAddress address, TransportConfig config)
        {
            Endpoint = address;
            _config = config;
            _tcpClient = new TcpClient();
            _tcpClient.Connect(address, _config.Port);

            Stream stream = _tcpClient.GetStream();
#if DEBUG_STREAM
            stream = new DebugStream(stream);
#endif

            _outputStream = stream;
            _inputStream = stream;

            for (byte idx = 0; idx < STREAMID_MAX; ++idx)
            {
                _availableStreamIds.Push(idx);
                _requestStates[idx].Lock = new object();
            }

            GetOptions();
            ReadifyConnection();
        }

        public void Dispose()
        {
            lock (_globalLock)
            {
                _tcpClient.SafeDispose();
            }
        }

        public IPAddress Endpoint { get; private set; }

        public Task<IEnumerable<object>> Execute(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader)
        {
            byte streamId;
            lock (_globalLock)
            {
                while (0 == _availableStreamIds.Count)
                {
                    ThrowClientRequestAbortedIfConnectionFailed();
                    Monitor.Wait(_globalLock);
                }
                ThrowClientRequestAbortedIfConnectionFailed();

                // get the stream id and initialize async reader context
                streamId = _availableStreamIds.Pop();

                // startup a new read task
                _currReadTask = null != _currReadTask
                                        ? _currReadTask.ContinueWith(_ => ReadNextFrameHeader())
                                        : Task.Factory.StartNew(ReadNextFrameHeader);
            }

            // start the async request
            var taskWrite = Task.Factory.StartNew(() => WriteNextFrame(writer, reader, streamId));
            return taskWrite;
        }

        public event EventHandler<FailureEventArgs> OnFailure;

        private IEnumerable<object> WriteNextFrame(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader, byte streamId)
        {
            // acquire the global lock to write the request
            lock (_globalLock)
            {
                ThrowClientRequestAbortedIfConnectionFailed();

                using (FrameWriter frameWriter = new FrameWriter(_outputStream, streamId))
                    writer(frameWriter);
            }

            // return a promise to stream results
            return StreamResultsThenReleaseStreamId(reader, streamId);
        }

        // *must* be called under _globalLock
        private void ThrowClientRequestAbortedIfConnectionFailed()
        {
            if (_failed)
            {
                throw new ClientRequestAbortedException();
            }
        }

        private IEnumerable<object> StreamResultsThenReleaseStreamId(Func<FrameReader, IEnumerable<object>> reader, byte streamId)
        {
            // we are completely client side there (ie: running on the thread of the client)
            // we have first to grab the request lock to avoid a race with async reader
            // and find if the async reader has started to read the frame
            lock (_requestStates[streamId].Lock)
            {
                try
                {
                    // if the reader has not read this stream id then just wait for a notification
                    if (!_requestStates[streamId].ReadBegan)
                    {
                        Monitor.Wait(_requestStates[streamId].Lock);
                    }

                    lock (_globalLock)
                    {
                        // release stream id (since result streaming has started)
                        _availableStreamIds.Push(streamId);
                        Monitor.Pulse(_globalLock);

                        ThrowClientRequestAbortedIfConnectionFailed();
                    }

                    // yield all rows - no lock required on input stream since we are the only one allowed to read
                    using (FrameReader frameReader = FrameReader.ReadBody(_inputStream))
                    {
                        foreach (object row in EnumerableOrEmptyEnumerable(reader(frameReader)))
                        {
                            yield return row;
                        }                            
                    }
                }
                finally
                {
                    // wake up the async reader
                    _requestStates[streamId].ReadBegan = false;
                    Monitor.Pulse(_requestStates[streamId].Lock);
                }
            }
        }

        private static IEnumerable<object> EnumerableOrEmptyEnumerable(IEnumerable<object> enumerable)
        {
            return enumerable ?? Enumerable.Empty<object>();
        }

        private void ReadNextFrameHeader()
        {
            try
            {
                // read stream id - we are the only one reading so no lock required
                byte streamId = FrameReader.ReadStreamId(_inputStream);

                // acquire request lock
                lock (_requestStates[streamId].Lock)
                {
                    // flip the status flag (write barrier in Pulse below)
                    _requestStates[streamId].ReadBegan = true;

                    // hand off the reading of the body to the request handler
                    Monitor.Pulse(_requestStates[streamId].Lock);

                    // wait for request handler to complete
                    Monitor.Wait(_requestStates[streamId].Lock);
                }
            }
            catch (Exception ex)
            {
                GeneralFailure(ex);
            }
        }

        private void GeneralFailure(Exception ex)
        {
            lock (_globalLock)
            {
                if (_failed)
                {
                    return;
                }

                _failed = true;

                // currently running request/response will be abruptly terminated
                Dispose();

                // release all pending client waiting for a response
                foreach (RequestState requestState in _requestStates)
                {
                    lock (requestState.Lock)
                    {
                        Monitor.Pulse(requestState.Lock);
                    }
                }
                Monitor.Pulse(_globalLock);

                if (null != OnFailure)
                {
                    FailureEventArgs failureEventArgs = new FailureEventArgs(ex);
                    OnFailure(this, failureEventArgs);
                }
            }
        }

        private void GetOptions()
        {
            Action<IFrameWriter> writer = CQLCommandHelpers.WriteOptions;
            Func<IFrameReader, IEnumerable<object>> reader = fr =>
                {
                    CQLCommandHelpers.ReadOptions(fr);
                    return null;
                };

            Execute(writer, reader).Result.Count();
        }

        private void ReadifyConnection()
        {
            Action<IFrameWriter> writer = fw => CQLCommandHelpers.WriteReady(fw, _config.CqlVersion);
            Func<IFrameReader, IEnumerable<object>> reader = fr => new object[] {CQLCommandHelpers.ReadReady(fr)};
            bool authenticate = Execute(writer, reader).Result.Cast<bool>().Single();
            if (authenticate)
            {
                Authenticate();
            }
        }

        private void Authenticate()
        {
            if (null == _config.User || null == _config.Password)
            {
                throw new InvalidCredentialException();
            }

            Action<IFrameWriter> writer = fw => CQLCommandHelpers.WriteAuthenticate(fw, _config.User, _config.Password);
            Func<IFrameReader, IEnumerable<object>> reader = fr =>
                {
                    CQLCommandHelpers.ReadAuthenticate(fr);
                    return null;
                };

            Execute(writer, reader).Result.Count();
        }

        private struct RequestState
        {
            public object Lock;

            public bool ReadBegan;
        }
    }
}