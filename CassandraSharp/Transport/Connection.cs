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
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;
    using CassandraSharp.Utils;

    internal partial class Connection : IConnection,
                                        IDisposable
    {
        private const byte MAX_STREAMID = 0x80;

        private readonly Stack<byte> _availableStreamIds = new Stack<byte>(MAX_STREAMID);

        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

        private readonly TransportConfig _config;

        private readonly object _globalLock = new object();

        private readonly IInstrumentation _instrumentation;

        private readonly ILogger _logger;

        private readonly QueryInfo[] _queryInfos = new QueryInfo[MAX_STREAMID];

        private readonly Socket _socket;

        private readonly bool _streaming;

        private readonly TcpClient _tcpClient;

        public Connection(IPAddress address, TransportConfig config, ILogger logger, IInstrumentation instrumentation)
        {
            Endpoint = address;
            _config = config;
            _logger = logger;
            _instrumentation = instrumentation;
            _tcpClient = new TcpClient();
            _tcpClient.Connect(address, _config.Port);
            _streaming = config.Streaming;

            for (byte idx = 0; idx < MAX_STREAMID; ++idx)
            {
                _availableStreamIds.Push(idx);
            }

            _socket = _tcpClient.Client;

            // start a new read task
            Task.Factory.StartNew(ReadNextFrameHeader, _cancellation.Token);

            // readify the connection
            _logger.Debug("Readyfying connection for {0}", Endpoint);
            //GetOptions();
            ReadifyConnection();
            _logger.Debug("Connection to {0} is ready", Endpoint);
        }

        public IPAddress Endpoint { get; private set; }

        public Task<IEnumerable<object>> Execute(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader, ExecutionFlags executionFlags,
                                                 InstrumentationToken instrumentationToken)
        {
            _instrumentation.ClientQuery(instrumentationToken);

            Task<IEnumerable<object>> taskRead;
            byte streamId;
            lock (_globalLock)
            {
                while (0 == _availableStreamIds.Count)
                {
                    _logger.Debug("Waiting for available stream id for {0}", Endpoint);
                    _cancellation.Token.ThrowIfCancellationRequested();
                    Monitor.Wait(_globalLock);
                }
                _cancellation.Token.ThrowIfCancellationRequested();

                // get the stream id and initialize async reader context
                streamId = _availableStreamIds.Pop();

                // promise to stream results
                taskRead = new Task<IEnumerable<object>>(() => ReadResultStream(streamId, reader));
                _queryInfos[streamId].InstrumentationToken = instrumentationToken;
                _queryInfos[streamId].ReadTask = taskRead;

                _instrumentation.ClientConnectionInfo(instrumentationToken, Endpoint, streamId);
            }
            _logger.Debug("Using stream {0}@{1}", streamId, Endpoint);

            // write the request asynchronously
            StartWriteNextFrame(writer, streamId, executionFlags);

            return taskRead;
        }

        public event EventHandler<FailureEventArgs> OnFailure;

        public void Dispose()
        {
            lock (_globalLock)
            {
                OnFailure = null;

                // wake up clients waiting for a stream id (so they can cancel themselves)
                _cancellation.Cancel();
                Monitor.Pulse(_globalLock);

                // cancel pending readers
                Exception cancelException = new OperationCanceledException();
                for (byte idx = 0; idx < MAX_STREAMID; ++idx)
                {
                    if (!_availableStreamIds.Contains(idx))
                    {
                        _instrumentation.ClientTrace(_queryInfos[idx].InstrumentationToken, EventType.Cancellation);

                        _queryInfos[idx].Exception = cancelException;
                        _queryInfos[idx].ReadTask.RunSynchronously();
                    }
                }

                _logger.Debug("Connection to {0} is being disposed", Endpoint);
                _tcpClient.SafeDispose();
                //_cancellation.SafeDispose();
            }
        }

        private IEnumerable<object> ReadResultStream(byte streamId, Func<IFrameReader, IEnumerable<object>> reader)
        {
            IFrameReader frameReader = _queryInfos[streamId].FrameReader;
            try
            {
                // forward error if any
                Exception exception = _queryInfos[streamId].Exception ?? frameReader.ResponseException;
                if (null != exception)
                {
                    exception.RethrowPreserveStackTrace();
                }

                return new ResultStreamEnumerable(this, reader, frameReader, streamId);
            }
            catch (Exception ex)
            {
                HandleFrameTerminaison(ex, frameReader, streamId);
                throw;
            }
        }

        private void StartWriteNextFrame(Action<IFrameWriter> writer, byte streamId, ExecutionFlags executionFlags)
        {
            Task.Factory.StartNew(() => WriteNextFrame(writer, streamId), _cancellation.Token);
        }

        private void WriteNextFrame(Action<IFrameWriter> writer, byte streamId)
        {
            try
            {
                InstrumentationToken token = _queryInfos[streamId].InstrumentationToken;
                _instrumentation.ClientTrace(token, EventType.BeginWrite);

                // acquire the global lock to write the request
                _logger.Debug("Starting writing frame for stream {0}@{1}", streamId, Endpoint);
                bool tracing = 0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing);
                using (BufferingFrameWriter bufferingFrameWriter = new BufferingFrameWriter(streamId, tracing))
                {
                    writer(bufferingFrameWriter);

                    lock (_globalLock)
                    {
                        bufferingFrameWriter.SendFrame(_socket);
                    }
                }

                _logger.Debug("Done writing frame for stream {0}@{1}", streamId, Endpoint);

                _instrumentation.ClientTrace(token, EventType.EndWrite);
            }
            catch (Exception ex)
            {
                _queryInfos[streamId].Exception = ex;
                _queryInfos[streamId].ReadTask.RunSynchronously();
            }
        }

        private void ReadNextFrameHeader()
        {
            IFrameReader frameReader = null;
            byte streamId = 0xFF;
            try
            {
                do
                {
                    frameReader = _streaming
                                          ? new StreamingFrameReader(_socket)
                                          : new BufferingFrameReader(_socket);
                    streamId = frameReader.StreamId;
                    _queryInfos[streamId].FrameReader = frameReader;

                    InstrumentationToken token = _queryInfos[streamId].InstrumentationToken;
                    _instrumentation.ClientTrace(token, EventType.BeginRead);

                    // NOTE: the task is running just to return an IEnumerable<object> to the client
                    //       so it's better to execute it on our context immediately
                    _queryInfos[streamId].ReadTask.RunSynchronously();

                    // if we are not streaming we can wait for a new frame
                } while (!_streaming);
            }
            catch (Exception ex)
            {
                if (null != frameReader)
                {
                    _queryInfos[streamId].Exception = ex;
                    _queryInfos[streamId].ReadTask.RunSynchronously();
                }
                else
                {
                    HandleFrameTerminaison(ex, null, streamId);
                }
            }
        }

        private void HandleFrameTerminaison(Exception ex, IFrameReader frameReader, byte streamId)
        {
            if (null != ex)
            {
                _logger.Debug("HandleFrameTerminaison notified with exception {0}", ex);
            }

            bool isFatal = ex is IOException
                           || ex is SocketException
                           || ex is OperationCanceledException
                           || null == frameReader;
            if (! isFatal)
            {
                InstrumentationToken token = _queryInfos[streamId].InstrumentationToken;

                // release streamId now
                lock (_globalLock)
                {
                    _queryInfos[streamId] = new QueryInfo();

                    // release stream id (since result streaming has started)
                    _availableStreamIds.Push(streamId);
                    Monitor.Pulse(_globalLock);
                }

                _instrumentation.ClientTrace(token, EventType.EndRead);

                frameReader.SafeDispose();

                if (_streaming)
                {
                    Task.Factory.StartNew(ReadNextFrameHeader, _cancellation.Token);
                }

                if (0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing) && frameReader.TraceId.HasValue)
                {
                    TracingSession tracingSession = this.GetTracingSession(frameReader.TraceId.Value);
                    _instrumentation.ServerTrace(token, tracingSession);
                }
                return;
            }

            lock (_globalLock)
            {
                _logger.Error("Connection to {0} is broken", Endpoint);

                if (null != OnFailure)
                {
                    FailureEventArgs failureEventArgs = new FailureEventArgs(ex);
                    OnFailure(this, failureEventArgs);
                }
            }
        }

//        private void GetOptions()
//        {
//            Action<IFrameWriter> writer = CQLCommandHelpers.WriteOptions;
//            Func<IFrameReader, IEnumerable<object>> reader = fr =>
//                {
//                    CQLCommandHelpers.ReadOptions(fr);
//                    return null;
//                };

//// ReSharper disable ReturnValueOfPureMethodIsNotUsed
//            Execute(writer, reader, ExecutionFlags.None).Result.Count();
//// ReSharper restore ReturnValueOfPureMethodIsNotUsed
//        }

        private void ReadifyConnection()
        {
            Action<IFrameWriter> writer = fw => CQLCommandHelpers.WriteReady(fw, _config.CqlVersion);
            Func<IFrameReader, IEnumerable<object>> reader = fr => new object[] {CQLCommandHelpers.ReadReady(fr)};
            InstrumentationToken readyToken = InstrumentationToken.Create(RequestType.Ready, ExecutionFlags.None);
            bool authenticate = (bool) Execute(writer, reader, ExecutionFlags.None, readyToken).Result.Single();
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

// ReSharper disable ReturnValueOfPureMethodIsNotUsed
            InstrumentationToken authToken = InstrumentationToken.Create(RequestType.Authenticate, ExecutionFlags.None);
            Execute(writer, reader, ExecutionFlags.None, authToken).Result.Count();
// ReSharper restore ReturnValueOfPureMethodIsNotUsed
        }

        private struct QueryInfo
        {
            public Exception Exception;

            public IFrameReader FrameReader;

            public InstrumentationToken InstrumentationToken;

            public Task<IEnumerable<object>> ReadTask;
        }
    }
}