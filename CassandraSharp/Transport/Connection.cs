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
    using CassandraSharp.Utils;

    internal class Connection : IConnection
    {
        private const byte MAX_STREAMID = 0x80;

        private readonly Stack<byte> _availableStreamIds = new Stack<byte>(MAX_STREAMID);

        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

        private readonly TransportConfig _config;

        private readonly object _globalLock = new object();

        private readonly Stream _inputStream;

        private readonly ILogger _logger;

        private readonly Stream _outputStream;

        private readonly Task<IEnumerable<object>>[] _readers = new Task<IEnumerable<object>>[MAX_STREAMID];

        private readonly TcpClient _tcpClient;

        public Connection(IPAddress address, TransportConfig config, ILogger logger)
        {
            Endpoint = address;
            _config = config;
            _logger = logger;
            _tcpClient = new TcpClient();
            _tcpClient.Connect(address, _config.Port);

            Stream stream = _tcpClient.GetStream();
#if DEBUG_STREAM
            stream = new DebugStream(stream);
#endif

            _outputStream = stream;
            _inputStream = stream;

            for (byte idx = 0; idx < MAX_STREAMID; ++idx)
            {
                _availableStreamIds.Push(idx);
            }

            // start a new read task
            Task.Factory.StartNew(ReadNextFrameHeader, _cancellation.Token);

            // readify the connection
            _logger.Debug("Readyfying connection for {0}", Endpoint);
            GetOptions();
            ReadifyConnection();
            _logger.Debug("Connection to {0} is ready", Endpoint);
        }

        public IPAddress Endpoint { get; private set; }

        public Task<IEnumerable<object>> Execute(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<object>> reader)
        {
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
                taskRead = CreateReadNextFrame(reader, streamId);
                _readers[streamId] = taskRead;
            }
            _logger.Debug("Using stream {0}@{1}", streamId, Endpoint);

            // write the request asynchronously
            StartWriteNextFrame(writer, streamId);

            return taskRead;
        }

        public event EventHandler<FailureEventArgs> OnFailure;

        public void Dispose()
        {
            _logger.Debug("Connection to {0} is being disposed", Endpoint);
            OnFailure = null;
            _tcpClient.SafeDispose();
            _cancellation.SafeDispose();
        }

        private Task<IEnumerable<object>> CreateReadNextFrame(Func<IFrameReader, IEnumerable<object>> reader, byte streamId)
        {
            return new Task<IEnumerable<object>>(() => StreamResultsThenReleaseStreamId(reader, streamId));
        }

        private IEnumerable<object> StreamResultsThenReleaseStreamId(Func<FrameReader, IEnumerable<object>> reader, byte streamId)
        {
            try
            {
                _logger.Debug("Starting reading stream {0}@{1}", streamId, Endpoint);
                lock (_globalLock)
                {
                    // release stream id (since result streaming has started)
                    _availableStreamIds.Push(streamId);
                    Monitor.Pulse(_globalLock);
                }

                // yield all rows - no lock required on input stream since we are the only one allowed to read
                using (FrameReader frameReader = FrameReader.ReadBody(_inputStream, _config.Streaming))
                {
                    foreach (object row in EnumerableOrEmptyEnumerable(reader(frameReader)))
                    {
                        yield return row;
                    }
                }

                _logger.Debug("Done reading stream {0}@{1}", streamId, Endpoint);
            }
            finally
            {
                Task.Factory.StartNew(ReadNextFrameHeader, _cancellation.Token);
            }
        }

        private void StartWriteNextFrame(Action<IFrameWriter> writer, byte streamId)
        {
            Task.Factory.StartNew(() => WriteNextFrame(writer, streamId), _cancellation.Token);
        }

        private void WriteNextFrame(Action<IFrameWriter> writer, byte streamId)
        {
            // acquire the global lock to write the request
            _logger.Debug("Starting writing frame for stream {0}@{1}", streamId, Endpoint);
            lock (_globalLock)
            {
                using (FrameWriter frameWriter = new FrameWriter(_outputStream, streamId))
                    writer(frameWriter);
            }

            _logger.Debug("Done writing frame for stream {0}@{1}", streamId, Endpoint);
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
                _readers[streamId].RunSynchronously();
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
                _logger.Error("Connection to {0} is broken", Endpoint);

                _cancellation.Cancel();

                // wake up eventually client waiting for a stream id
                Monitor.Pulse(_globalLock);

                if (null != OnFailure)
                {
                    FailureEventArgs failureEventArgs = new FailureEventArgs(ex);
                    OnFailure(this, failureEventArgs);
                }

                // currently running request/response will be abruptly terminated
                Dispose();
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

// ReSharper disable ReturnValueOfPureMethodIsNotUsed
            Execute(writer, reader).Result.Count();
// ReSharper restore ReturnValueOfPureMethodIsNotUsed
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

// ReSharper disable ReturnValueOfPureMethodIsNotUsed
            Execute(writer, reader).Result.Count();
// ReSharper restore ReturnValueOfPureMethodIsNotUsed
        }
    }
}