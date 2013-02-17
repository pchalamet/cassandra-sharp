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

namespace CassandraSharp.Transport.Stream
{
    using System;
    using System.Collections.Generic;
    using System.Net.Sockets;
    using System.Text;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;

    internal class StreamingFrameReader : IFrameReader
    {
        private readonly Socket _socket;

        private readonly byte[] _tempBuffer = new byte[16];

        protected int FrameBytesLeft;

        public StreamingFrameReader(Socket socket)
        {
            _socket = socket;
            SocketReceiveBuffer(_tempBuffer, 0, 8);

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

            StreamId = _tempBuffer[2];

            MessageOpcode = (MessageOpcodes) _tempBuffer[3];
            FrameBytesLeft = _tempBuffer.ToInt(4);

            bool tracing = 0 != (flags & FrameHeaderFlags.Tracing);
            if (tracing)
            {
                SocketReceiveBuffer(_tempBuffer, 0, 16);
                FrameBytesLeft -= 16;

                TraceId = _tempBuffer.ToGuid();
            }

            if (MessageOpcodes.Error == MessageOpcode)
            {
                ResponseException = CreateExceptionFromError();
            }
        }

        public Guid TraceId { get; private set; }

        public virtual void Dispose()
        {
            while (0 < FrameBytesLeft)
            {
                int left = Math.Min(FrameBytesLeft, _tempBuffer.Length);
                ReceiveBuffer(_tempBuffer, 0, left);
            }
        }

        public byte StreamId { get; private set; }

        public MessageOpcodes MessageOpcode { get; private set; }

        public Exception ResponseException { get; private set; }

        public byte ReadByte()
        {
            ReceiveBuffer(_tempBuffer, 0, sizeof (byte));
            return _tempBuffer[0];
        }

        public short ReadShort()
        {
            ReceiveBuffer(_tempBuffer, 0, sizeof (short));
            return _tempBuffer.ToShort(0);
        }

        public int ReadInt()
        {
            ReceiveBuffer(_tempBuffer, 0, sizeof (int));
            return _tempBuffer.ToInt(0);
        }

        public string ReadString()
        {
            short len = ReadShort();
            if (0 == len)
            {
                return string.Empty;
            }

            byte[] bufStr = new byte[len];
            ReceiveBuffer(bufStr);
            string data = Encoding.UTF8.GetString(bufStr);
            return data;
        }

        public string[] ReadStringList()
        {
            short len = ReadShort();
            string[] data = new string[len];
            for (int i = 0; i < len; ++i)
            {
                data[i] = ReadString();
            }

            return data;
        }

        public byte[] ReadBytes()
        {
            int len = ReadInt();
            if (-1 == len)
            {
                return null;
            }

            byte[] data = new byte[len];
            ReceiveBuffer(data);
            return data;
        }

        public byte[] ReadShortBytes()
        {
            short len = ReadShort();
            byte[] data = new byte[len];
            ReceiveBuffer(data);
            return data;
        }

        public Dictionary<string, string[]> ReadStringMultimap()
        {
            short len = ReadShort();
            Dictionary<string, string[]> data = new Dictionary<string, string[]>(len);
            for (int i = 0; i < len; ++i)
            {
                string key = ReadString();
                string[] value = ReadStringList();
                data.Add(key, value);
            }

            return data;
        }

        protected void ReceiveBuffer(byte[] buffer)
        {
            ReceiveBuffer(buffer, 0, buffer.Length);
        }

        protected virtual void ReceiveBuffer(byte[] buffer, int offset, int len)
        {
            if (FrameBytesLeft < len)
            {
                throw new ArgumentException("ReceiveBuffer blocked read past frame");
            }

            FrameBytesLeft -= SocketReceiveBuffer(buffer, offset, len);
        }

        private int SocketReceiveBuffer(byte[] buffer, int offset, int len)
        {
            int read = 0;
            while (read != len)
            {
                read += _socket.Receive(buffer, offset + read, len - read, SocketFlags.None);
            }

            return read;
        }

        private Exception CreateExceptionFromError()
        {
            ErrorCodes code = (ErrorCodes) ReadInt();
            string msg = ReadString();

            switch (code)
            {
                case ErrorCodes.Unavailable:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) ReadShort();
                        int required = ReadInt();
                        int alive = ReadInt();
                        return new UnavailableException(msg, cl, required, alive);
                    }

                case ErrorCodes.WriteTimeout:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) ReadShort();
                        int received = ReadInt();
                        int blockFor = ReadInt();
                        string writeType = ReadString();
                        return new WriteTimeOutException(msg, cl, received, blockFor, writeType);
                    }

                case ErrorCodes.ReadTimeout:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) ReadShort();
                        int received = ReadInt();
                        int blockFor = ReadInt();
                        bool dataPresent = 0 != ReadByte();
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
                        string keyspace = ReadString();
                        string table = ReadString();
                        return new AlreadyExistsException(msg, keyspace, table);
                    }

                case ErrorCodes.Unprepared:
                    {
                        byte[] unknownId = ReadShortBytes();
                        return new UnpreparedException(msg, unknownId);
                    }

                default:
                    return new CassandraException(code, msg);
            }
        }
    }
}