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
    using System.Threading.Tasks;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;
    using CassandraSharp.Transport.Stream;
    using CassandraSharp.Utils;

    internal class ShortRunningConnection : IConnection,
                                            IDisposable
    {
        private const byte MAX_STREAMID = 0x80;

        private readonly Stack<byte> _availableStreamIds = new Stack<byte>(MAX_STREAMID);

        private readonly TransportConfig _config;

        private readonly IInstrumentation _instrumentation;

        private readonly object _lock = new object();

        private readonly ILogger _logger;

        private readonly Queue<QueryInfo> _pendingQueries = new Queue<QueryInfo>();

        private readonly QueryInfo[] _queryInfos = new QueryInfo[MAX_STREAMID];

        private readonly Socket _socket;

        private readonly TcpClient _tcpClient;

        private volatile bool _isClosed;

        public ShortRunningConnection(IPAddress address, TransportConfig config, ILogger logger, IInstrumentation instrumentation)
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
                        ReceiveTimeout = config.ReceiveTimeout,
                        SendTimeout = config.SendTimeout,
                        NoDelay = true,
                };
            _tcpClient.Connect(address, _config.Port);
            _socket = _tcpClient.Client;

            Task.Factory.StartNew(ReadResponseWorker, TaskCreationOptions.LongRunning);

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
            byte streamId;
            QueryInfo queryInfo = new QueryInfo(writer, reader, token, observer);
            lock (_lock)
            {
                if (0 == _availableStreamIds.Count)
                {
                    _pendingQueries.Enqueue(queryInfo);
                    return;
                }

                streamId = _availableStreamIds.Pop();
                _queryInfos[streamId] = queryInfo;
            }

            AsyncSendQuery(streamId, queryInfo);
        }

        public void Dispose()
        {
            Close(false);
        }

        private void Close(bool notifyFailure)
        {
            lock (_lock)
            {
                // if we have already failed then do nothing
                if (_isClosed)
                {
                    return;
                }

                _isClosed = true;
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
        }

        private void AsyncSendQuery(byte streamId, QueryInfo queryInfo)
        {
            Task.Factory.StartNew(() => SendQueryWorker(streamId, queryInfo));
        }

        private void SendQueryWorker(byte streamId, QueryInfo queryInfo)
        {
            try
            {
                SendQuery(streamId, queryInfo);
            }
            catch (Exception ex)
            {
                _logger.Fatal("Error while trying to send query '{0}': {1}", queryInfo.Token.Cql, ex);
                HandleError(ex);
            }
        }

        private void SendQuery(byte streamId, QueryInfo queryInfo)
        {
            try
            {
                InstrumentationToken token = queryInfo.Token;
                _instrumentation.ClientTrace(token, EventType.BeginWrite);

                // acquire the global lock to write the request
                _logger.Debug("Starting writing frame for stream {0}@{1}", streamId, Endpoint);
                bool tracing = 0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing);
                using (BufferingFrameWriter bufferingFrameWriter = new BufferingFrameWriter(tracing))
                {
                    queryInfo.Writer(bufferingFrameWriter);

                    lock (_lock)
                    {
                        bufferingFrameWriter.SendFrame(streamId, _socket);
                    }
                }

                _logger.Debug("Done writing frame for stream {0}@{1}", streamId, Endpoint);

                _instrumentation.ClientTrace(token, EventType.EndWrite);
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

                    // a streamId is available, we can enqueue a pending one if any
                    lock (_lock)
                    {
                        if (0 < _pendingQueries.Count)
                        {
                            QueryInfo newQueryInfo = _pendingQueries.Dequeue();
                            _queryInfos[streamId] = newQueryInfo;
                            AsyncSendQuery(streamId, newQueryInfo);
                        }
                        else
                        {
                            _availableStreamIds.Push(streamId);
                            _queryInfos[streamId] = null;
                        }
                    }

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