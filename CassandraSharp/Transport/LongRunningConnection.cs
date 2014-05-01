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
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.Config;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal sealed class LongRunningConnection : IConnection,
                                                  IDisposable
    {
        private const byte MAX_STREAMID = 0x80;

        private readonly Stack<byte> _availableStreamIds = new Stack<byte>();

        private readonly TransportConfig _config;

        private readonly KeyspaceConfig _keyspaceConfig;

        private readonly IInstrumentation _instrumentation;

        private readonly object _lock = new object();

        private readonly ILogger _logger;

        private readonly Queue<QueryInfo> _pendingQueries = new Queue<QueryInfo>();

        private readonly Action<QueryInfo, IFrameReader, bool> _pushResult;

        private readonly QueryInfo[] _queryInfos = new QueryInfo[MAX_STREAMID];

        private readonly Task _queryWorker;

        private readonly Task _responseWorker;

        private readonly Socket _socket;

        private readonly TcpClient _tcpClient;

        private bool _isClosed;

        public LongRunningConnection(IPAddress address, TransportConfig config, KeyspaceConfig keyspaceConfig, ILogger logger, IInstrumentation instrumentation)
        {
            try
            {
                for (byte streamId = 0; streamId < MAX_STREAMID; ++streamId)
                {
                    _availableStreamIds.Push(streamId);
                }

                _config = config;
                _keyspaceConfig = keyspaceConfig;
                _logger = logger;
                _instrumentation = instrumentation;

                Endpoint = address;
                DefaultConsistencyLevel = config.DefaultConsistencyLevel;
                DefaultExecutionFlags = config.DefaultExecutionFlags;

                _tcpClient = new TcpClient
                    {
                            ReceiveTimeout = _config.ReceiveTimeout,
                            SendTimeout = _config.SendTimeout,
                            NoDelay = true,
                            LingerState = {Enabled = true, LingerTime = 0},
                    };

                _tcpClient.Connect(address, _config.Port);
                _socket = _tcpClient.Client;

                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, _config.KeepAlive);
                if (_config.KeepAlive && 0 != _config.KeepAliveTime)
                {
                    SetTcpKeepAlive(_socket, _config.KeepAliveTime, 1000);
                }

                _pushResult = _config.ReceiveBuffering
                                      ? (Action<QueryInfo, IFrameReader, bool>) ((qi, fr, a) => Task.Factory.StartNew(() => PushResult(qi, fr, a)))
                                      : PushResult;
                _responseWorker = Task.Factory.StartNew(ReadResponseWorker, TaskCreationOptions.LongRunning);
                _queryWorker = Task.Factory.StartNew(SendQueryWorker, TaskCreationOptions.LongRunning);

                // readify the connection
                _logger.Debug("Readyfying connection for {0}", Endpoint);
                //GetOptions();
                ReadifyConnection();
                _logger.Debug("Connection to {0} is ready", Endpoint);
            }
            catch (Exception ex)
            {
                Dispose();

                _logger.Error("Failed building connection {0}", ex);
                throw;
            }
        }

        public IPAddress Endpoint { get; private set; }

        public ConsistencyLevel DefaultConsistencyLevel { get; private set; }

        public ExecutionFlags DefaultExecutionFlags { get; private set; }

        public event EventHandler<FailureEventArgs> OnFailure;

        public void Execute<T>(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<T>> reader, InstrumentationToken token,
                               IObserver<T> observer)
        {
            QueryInfo queryInfo = new QueryInfo<T>(writer, reader, token, observer);
            lock (_lock)
            {
                Monitor.Pulse(_lock);
                if (_isClosed)
                {
                    throw new OperationCanceledException();
                }

                _pendingQueries.Enqueue(queryInfo);
            }
        }

        public void Dispose()
        {
            Close(null);

            // wait for worker threads to gracefully shutdown
            //ExceptionExtensions.SafeExecute(() => _responseWorker.Wait());
            //ExceptionExtensions.SafeExecute(() => _queryWorker.Wait());
        }

        public static void SetTcpKeepAlive(Socket socket, int keepaliveTime, int keepaliveInterval)
        {
            // marshal the equivalent of the native structure into a byte array
            byte[] inOptionValues = new byte[12];

            int enable = 0 != keepaliveTime
                                 ? 1
                                 : 0;
            BitConverter.GetBytes(enable).CopyTo(inOptionValues, 0);
            BitConverter.GetBytes(keepaliveTime).CopyTo(inOptionValues, 4);
            BitConverter.GetBytes(keepaliveInterval).CopyTo(inOptionValues, 8);

            // write SIO_VALS to Socket IOControl
            socket.IOControl(IOControlCode.KeepAliveValues, inOptionValues, null);
        }

        private void Close(Exception ex)
        {
            // already in close state ?
            lock (_lock)
            {
                Monitor.Pulse(_lock);
                if (_isClosed)
                {
                    return;
                }

                // abort all pending queries
                OperationCanceledException canceledException = new OperationCanceledException();
                for (int i = 0; i < _queryInfos.Length; ++i)
                {
                    var queryInfo = _queryInfos[i];
                    if (null != queryInfo)
                    {
                        queryInfo.NotifyError(canceledException);
                        _instrumentation.ClientTrace(queryInfo.Token, EventType.Cancellation);

                        _queryInfos[i] = null;
                    }
                }

				foreach (var queryInfo in _pendingQueries)
				{
					queryInfo.NotifyError(canceledException);
					_instrumentation.ClientTrace(queryInfo.Token, EventType.Cancellation);
				}

				_pendingQueries.Clear();

				_isClosed = true;
            }

            // we have now the guarantee this instance is destroyed once
            _tcpClient.SafeDispose();

            if (null != ex && null != OnFailure)
            {
                _logger.Fatal("Failed with error : {0}", ex);

				FailureEventArgs failureEventArgs = new FailureEventArgs(ex);
                OnFailure(this, failureEventArgs);
                OnFailure = null;
            }
        }

        private void SendQueryWorker()
        {
            try
            {
                SendQuery();
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }
        }

        private void SendQuery()
        {
            while (true)
            {
                QueryInfo queryInfo;
                lock (_lock)
                {
                    while (!_isClosed && 0 == _pendingQueries.Count)
                    {
                        Monitor.Wait(_lock);
                    }
                    if (_isClosed)
                    {
                        Monitor.Pulse(_lock);
                        return;
                    }

                    queryInfo = _pendingQueries.Dequeue();
                }

                try
                {
                    // acquire the global lock to write the request
                    InstrumentationToken token = queryInfo.Token;
                    bool tracing = 0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing);
                    using (BufferingFrameWriter bufferingFrameWriter = new BufferingFrameWriter(tracing))
                    {
                        queryInfo.Write(bufferingFrameWriter);

                        byte streamId;
                        lock (_lock)
                        {
                            while (!_isClosed && 0 == _availableStreamIds.Count)
                            {
                                Monitor.Wait(_lock);
                            }
                            if (_isClosed)
                            {
								queryInfo.NotifyError(new OperationCanceledException());
								_instrumentation.ClientTrace(token, EventType.Cancellation);
                                Monitor.Pulse(_lock);
                                return;
                            }

                            streamId = _availableStreamIds.Pop();
                        }

                        _logger.Debug("Starting writing frame for stream {0}@{1}", streamId, Endpoint);
                        _instrumentation.ClientTrace(token, EventType.BeginWrite);

                        _queryInfos[streamId] = queryInfo;
                        bufferingFrameWriter.SendFrame(streamId, _socket);

                        _logger.Debug("Done writing frame for stream {0}@{1}", streamId, Endpoint);
                        _instrumentation.ClientTrace(token, EventType.EndWrite);
                    }
                }
                catch (Exception ex)
                {
                    queryInfo.NotifyError(ex);

                    if (IsStreamInBadState(ex))
                    {
                        throw;
                    }
                }
            }
        }

        private void ReadResponseWorker()
        {
            try
            {
                ReadResponse();
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }
        }

        private void ReadResponse()
        {
            while (true)
            {
                IFrameReader frameReader = null;
                try
                {
                    frameReader = _config.ReceiveBuffering
                                          ? new BufferingFrameReader(_socket)
                                          : new StreamingFrameReader(_socket);

                    QueryInfo queryInfo = GetAndReleaseQueryInfo(frameReader);

                    _pushResult(queryInfo, frameReader, _config.ReceiveBuffering);
                }
                catch (Exception)
                {
                    frameReader.SafeDispose();
                    throw;
                }
            }
        }

        private QueryInfo GetAndReleaseQueryInfo(IFrameReader frameReader)
        {
            QueryInfo queryInfo;
            byte streamId = frameReader.StreamId;
            lock (_lock)
            {
                Monitor.Pulse(_lock);
                if (_isClosed)
                {
                    throw new OperationCanceledException();
                }

                queryInfo = _queryInfos[streamId];
                _queryInfos[streamId] = null;
                _availableStreamIds.Push(streamId);
            }
            return queryInfo;
        }

        private void PushResult(QueryInfo queryInfo, IFrameReader frameReader, bool isAsync)
        {
            try
            {
                _instrumentation.ClientTrace(queryInfo.Token, EventType.BeginRead);
                try
                {
                    if (null != frameReader.ResponseException)
                    {
                        throw frameReader.ResponseException;
                    }

                    queryInfo.Push(frameReader);
                }
                catch (Exception ex)
                {
                    queryInfo.NotifyError(ex);

                    if (IsStreamInBadState(ex))
                    {
                        throw;
                    }
                }

                _instrumentation.ClientTrace(queryInfo.Token, EventType.EndRead);

                InstrumentationToken token = queryInfo.Token;
                if (0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing))
                {
                    _instrumentation.ServerTrace(token, frameReader.TraceId);
                }
            }
            catch (Exception ex)
            {
                if (isAsync)
                {
                    HandleError(ex);
                }
            }
            finally
            {
                if (isAsync)
                {
                    frameReader.SafeDispose();
                }
            }
        }

        private static bool IsStreamInBadState(Exception ex)
        {
            bool isFatal = ex is SocketException || ex is IOException || ex is TimeOutException;
            return isFatal;
        }

        private void HandleError(Exception ex)
        {
            Close(ex);
        }

        private void GetOptions()
        {
            var obsOptions = new CreateOptionsQuery(this, ConsistencyLevel.ONE, ExecutionFlags.None).AsFuture();
            obsOptions.Wait();
        }

        private void ReadifyConnection()
        {
            var obsReady = new ReadyQuery(this, ConsistencyLevel.ONE, ExecutionFlags.None, _config.CqlVersion).AsFuture();
            bool authenticate = obsReady.Result.Single();
            if (authenticate)
            {
                Authenticate();
            }

            if (!string.IsNullOrWhiteSpace(_keyspaceConfig.Name))
            {
                SetupKeyspace();
            }
        }

        private void Authenticate()
        {
            if (null == _config.User || null == _config.Password)
            {
                throw new InvalidCredentialException();
            }

            var obsAuth = new AuthenticateQuery(this, ConsistencyLevel.ONE, ExecutionFlags.None, _config.User, _config.Password).AsFuture();
            if (! obsAuth.Result.Single())
            {
                throw new InvalidCredentialException();
            }
        }

        private void SetupKeyspace()
        {
            var setKeyspaceQuery = new SetKeyspaceQuery(this, _keyspaceConfig.Name);

            try
            {
                setKeyspaceQuery.AsFuture().Wait();
            }
            catch
            {
                new CreateKeyspaceQuery(
                        this,
                        _keyspaceConfig.Name,
                        _keyspaceConfig.Replication.Options,
                        _keyspaceConfig.DurableWrites).AsFuture().Wait();
                setKeyspaceQuery.AsFuture().Wait();
            }

            _logger.Debug("Set default keyspace to {0}", _keyspaceConfig.Name);
        }

        private abstract class QueryInfo
        {
            protected QueryInfo(InstrumentationToken token)
            {
                Token = token;
            }

            public InstrumentationToken Token { get; private set; }

            public abstract void Write(IFrameWriter frameWriter);

            public abstract void Push(IFrameReader frameReader);

            public abstract void NotifyError(Exception ex);
        }

        private class QueryInfo<T> : QueryInfo
        {
            public QueryInfo(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<T>> reader,
                             InstrumentationToken token, IObserver<T> observer)
                    : base(token)
            {
                Writer = writer;
                Reader = reader;
                Observer = observer;
            }

            private Func<IFrameReader, IEnumerable<T>> Reader { get; set; }

            private Action<IFrameWriter> Writer { get; set; }

            private IObserver<T> Observer { get; set; }

            public override void Write(IFrameWriter frameWriter)
            {
                Writer(frameWriter);
            }

            public override void Push(IFrameReader frameReader)
            {
                IEnumerable<T> data = Reader(frameReader);
                foreach (T datum in data)
                {
                    Observer.OnNext(datum);
                }
                Observer.OnCompleted();
            }

            public override void NotifyError(Exception ex)
            {
                Observer.OnError(ex);
            }
        }
    }
}