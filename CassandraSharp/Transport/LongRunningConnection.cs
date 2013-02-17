// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Security.Authentication;
    using System.Threading;
    using System.Threading.Tasks;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;
    using CassandraSharp.Transport.Stream;
    using CassandraSharp.Utils;

    internal class LongRunningConnection : IConnection,
                                           IDisposable
    {
        private const byte MAX_STREAMID = 0x80;

        private readonly ConcurrentStack<byte> _availableStreamIds = new ConcurrentStack<byte>();

        private readonly TransportConfig _config;

        private readonly IInstrumentation _instrumentation;

        private readonly ILogger _logger;

        private readonly ConcurrentQueue<QueryInfo> _pendingQueries = new ConcurrentQueue<QueryInfo>();

        private readonly AutoResetEvent _pulseWriter = new AutoResetEvent(false);

        private readonly QueryInfo[] _queryInfos = new QueryInfo[MAX_STREAMID];

        private readonly Socket _socket;

        private readonly TcpClient _tcpClient;

        private int _isClosed;

        public LongRunningConnection(IPAddress address, TransportConfig config, ILogger logger, IInstrumentation instrumentation)
        {
            for (byte streamId = 0; streamId < MAX_STREAMID; ++streamId)
            {
                _availableStreamIds.Push(streamId);
            }

            _config = config;
            _logger = logger;
            _instrumentation = instrumentation;

            Endpoint = address;

            _tcpClient = new TcpClient
                {
                        ReceiveTimeout = _config.ReceiveTimeout,
                        SendTimeout = _config.SendTimeout,
                        NoDelay = true,
                };
            _tcpClient.Connect(address, _config.Port);
            _socket = _tcpClient.Client;

            Task.Factory.StartNew(ReadResponseWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(SendQueryWorker, TaskCreationOptions.LongRunning);

            // readify the connection
            _logger.Debug("Readyfying connection for {0}", Endpoint);
            //GetOptions();
            ReadifyConnection();
            _logger.Debug("Connection to {0} is ready", Endpoint);
        }

        public IPAddress Endpoint { get; private set; }

        public event EventHandler<FailureEventArgs> OnFailure;

        public void Execute(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader, InstrumentationToken token,
                            IObserver<object> observer)
        {
            QueryInfo queryInfo = new QueryInfo(writer, reader, token, observer);
            _pendingQueries.Enqueue(queryInfo);
            _pulseWriter.Set();
        }

        public void Dispose()
        {
            Close(false);
        }

        private void Close(bool notifyFailure)
        {
            // already in close state ?
            if (1 == Interlocked.Exchange(ref _isClosed, 1))
            {
                return;
            }

            _pulseWriter.Set();

            _tcpClient.SafeDispose();

            OperationCanceledException canceledException = new OperationCanceledException();
            foreach (QueryInfo queryInfo in _queryInfos.Where(queryInfo => null != queryInfo))
            {
                queryInfo.Observer.OnError(canceledException);
                _instrumentation.ClientTrace(queryInfo.Token, EventType.Cancellation);
            }

            if (notifyFailure && null != OnFailure)
            {
                FailureEventArgs failureEventArgs = new FailureEventArgs(null);
                OnFailure(this, failureEventArgs);
            }

            OnFailure = null;
        }

        private void SendQueryWorker()
        {
            try
            {
                SendQuery();
            }
            catch (Exception ex)
            {
                _logger.Fatal("Error while trying to send query : {0}", ex);
                HandleError(ex);
            }
        }

        private void SendQuery()
        {
            while (0 == Thread.VolatileRead(ref _isClosed))
            {
                QueryInfo queryInfo;
                if (_pendingQueries.TryDequeue(out queryInfo))
                {
                    try
                    {
                        // acquire the global lock to write the request
                        InstrumentationToken token = queryInfo.Token;
                        bool tracing = 0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing);
                        using (BufferingFrameWriter bufferingFrameWriter = new BufferingFrameWriter(tracing))
                        {
                            queryInfo.Writer(bufferingFrameWriter);

                            byte streamId;
                            while (!_availableStreamIds.TryPop(out streamId))
                            {
                                _pulseWriter.WaitOne();

                                if (0 != Thread.VolatileRead(ref _isClosed))
                                {
                                    throw new OperationCanceledException();
                                }
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
                        queryInfo.Observer.OnError(ex);
                        if (ex is SocketException || ex is IOException)
                        {
                            throw;
                        }
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
                _logger.Fatal("Error while trying to receive response: {0}", ex);
                HandleError(ex);
            }
        }

        private void ReadResponse()
        {
            while (true)
            {
                using (IFrameReader frameReader = new StreamingFrameReader(_socket))
                {
                    byte streamId = frameReader.StreamId;
                    QueryInfo queryInfo = _queryInfos[streamId];
                    _queryInfos[streamId] = null;
                    _availableStreamIds.Push(streamId);

                    _pulseWriter.Set();

                    _instrumentation.ClientTrace(queryInfo.Token, EventType.BeginRead);
                    IObserver<object> observer = queryInfo.Observer;
                    if (null == frameReader.ResponseException)
                    {
                        try
                        {
                            IEnumerable<object> data = queryInfo.Reader(frameReader);
                            foreach (object datum in data)
                            {
                                observer.OnNext(datum);
                            }
                            observer.OnCompleted();
                        }
                        catch (Exception ex)
                        {
                            observer.OnError(ex);
                            if (ex is SocketException || ex is IOException)
                            {
                                throw;
                            }
                        }
                    }
                    else
                    {
                        observer.OnError(frameReader.ResponseException);
                    }
                    _instrumentation.ClientTrace(queryInfo.Token, EventType.EndRead);
                }
            }
        }

        private void HandleError(Exception ex)
        {
            Close(true);
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
            IObservable<object> obsReady = CQLCommandHelpers.CreateReadyQuery(this, _config.CqlVersion);
            Task<IList<object>> res = obsReady.AsFuture();
            res.Wait();

            bool authenticate = (bool) res.Result.Single();
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

            IObservable<object> obsAuth = CQLCommandHelpers.CreateAuthenticateQuery(this, _config.User, _config.Password);
            obsAuth.AsFuture().Wait();
        }

        private class QueryInfo
        {
            public QueryInfo(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader,
                             InstrumentationToken token, IObserver<object> observer)
            {
                Writer = writer;
                Reader = reader;
                Token = token;
                Observer = observer;
            }

            public Func<IFrameReader, IEnumerable<object>> Reader { get; private set; }

            public InstrumentationToken Token { get; private set; }

            public Action<IFrameWriter> Writer { get; private set; }

            public IObserver<object> Observer { get; private set; }
        }
    }
}