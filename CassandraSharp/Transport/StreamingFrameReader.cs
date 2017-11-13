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
    using System.IO;
    using System.Net.Sockets;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using CassandraSharp.Utils.Stream;

    internal class StreamingFrameReader : IFrameReader
    {
        private readonly byte[] _tempBuffer = new byte[16];

        public StreamingFrameReader(Socket socket)
        {
            try
            {
                SocketReadOnlyStream.SocketReceiveBuffer(socket, _tempBuffer, 0, 9);

                FrameType version = (FrameType) _tempBuffer[0];
                if (0 == (version & FrameType.Response))
                {
                    throw new ArgumentException("Expecting response frame");
                }
                if (FrameType.ProtocolVersion != (version & FrameType.ProtocolVersionMask))
                {
                    throw new ArgumentException("Unknown protocol version");
                }

                FrameHeaderFlags flags = (FrameHeaderFlags) _tempBuffer[1];

                StreamId = (ushort)((_tempBuffer[2] << 8) | _tempBuffer[3]);

                MessageOpcode = (MessageOpcodes) _tempBuffer[4];
                int frameBytesLeft = _tempBuffer.ToInt(5);

                bool tracing = 0 != (flags & FrameHeaderFlags.Tracing);
                if (tracing)
                {
                    SocketReadOnlyStream.SocketReceiveBuffer(socket, _tempBuffer, 0, 16);
                    frameBytesLeft -= 16;

                    TraceId = _tempBuffer.ToGuid();
                }

// ReSharper disable DoNotCallOverridableMethodsInConstructor
                ReadOnlyStream = CreateStream(socket, frameBytesLeft);
// ReSharper restore DoNotCallOverridableMethodsInConstructor

                if (MessageOpcodes.Error == MessageOpcode)
                {
                    ResponseException = CreateExceptionFromError(ReadOnlyStream);
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        protected virtual Stream CreateStream(Socket socket, int frameBytesLeft)
        {
            return new SocketReadOnlyStream(socket, frameBytesLeft);
        }

        public void Dispose()
        {
            ReadOnlyStream.SafeDispose();
        }

        public Guid TraceId { get; private set; }

        public ushort StreamId { get; private set; }

        public MessageOpcodes MessageOpcode { get; private set; }

        public Exception ResponseException { get; private set; }

        public Stream ReadOnlyStream { get; protected set; }

        private static Exception CreateExceptionFromError(Stream stream)
        {
            ErrorCodes code = (ErrorCodes) stream.ReadInt();
            string msg = stream.ReadString();

            switch (code)
            {
                case ErrorCodes.Unavailable:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) stream.ReadUShort();
                        int required = stream.ReadInt();
                        int alive = stream.ReadInt();
                        return new UnavailableException(msg, cl, required, alive);
                    }

                case ErrorCodes.WriteTimeout:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) stream.ReadUShort();
                        int received = stream.ReadInt();
                        int blockFor = stream.ReadInt();
                        string writeType = stream.ReadString();
                        return new WriteTimeOutException(msg, cl, received, blockFor, writeType);
                    }

                case ErrorCodes.ReadTimeout:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) stream.ReadUShort();
                        int received = stream.ReadInt();
                        int blockFor = stream.ReadInt();
                        bool dataPresent = 0 != stream.ReadByte();
                        return new ReadTimeOutException(msg, cl, received, blockFor, dataPresent);
                    }

                case ErrorCodes.Syntax:
                    return new SyntaxException(msg);

                case ErrorCodes.Unauthorized:
                    return new UnauthorizedException(msg);

                case ErrorCodes.Invalid:
                    return new InvalidException(msg);

                case ErrorCodes.AlreadyExists:
                    {
                        string keyspace = stream.ReadString();
                        string table = stream.ReadString();
                        return new AlreadyExistsException(msg, keyspace, table);
                    }

                case ErrorCodes.Unprepared:
                    {
                        byte[] unknownId = stream.ReadShortBytes();
                        return new UnpreparedException(msg, unknownId);
                    }

                default:
                    return new CassandraException(code, msg);
            }
        }
    }
}