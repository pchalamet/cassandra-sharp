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
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal class FrameReader : IFrameReader,
                                 IDisposable
    {
        private readonly Stream _ms;

        private FrameReader(Stream stream, bool streaming)
        {
            try
            {
                MessageOpcode = (MessageOpcodes)stream.ReadByte();
                int bodyLen = stream.ReadInt();
                if (streaming)
                {
                    _ms = new WindowedReadStream(stream, bodyLen);
                }
                else
                {
                    byte[] buffer = new byte[bodyLen];
                    if (0 < bodyLen)
                    {
                        stream.Read(buffer, 0, bodyLen);
                        _ms = new MemoryStream(buffer);
                    }
                }

                if (MessageOpcodes.Error == MessageOpcode)
                {
                    ThrowError();
                }
            }
            catch
            {
                Dispose();
                throw;
            }
            
        }

        public void Dispose()
        {
            _ms.SafeDispose();
        }

        public MessageOpcodes MessageOpcode { get; private set; }

        public byte ReadByte()
        {
            return (byte) _ms.ReadByte();
        }

        public short ReadShort()
        {
            return _ms.ReadShort();
        }

        public int ReadInt()
        {
            return _ms.ReadInt();
        }

        public string ReadString()
        {
            return _ms.ReadString();
        }

        public string[] ReadStringList()
        {
            return _ms.ReadStringList();
        }

        public byte[] ReadBytes()
        {
            return _ms.ReadBytes();
        }

        public byte[] ReadShortBytes()
        {
            return _ms.ReadShortBytes();
        }

        public Dictionary<string, string[]> ReadStringMultimap()
        {
            return _ms.ReadStringMultimap();
        }

        public static FrameReader ReadBody(Stream stream, bool streaming)
        {
            return new FrameReader(stream, streaming);
        }

        public static byte ReadStreamId(Stream stream)
        {
            FrameType version = (FrameType) stream.ReadByte();
            if (0 == (version & FrameType.Response))
            {
                throw new ArgumentException("Expecting response frame");
            }
            if (FrameType.ProtocolVersion != (version & FrameType.ProtocolVersionMask))
            {
                throw new ArgumentException("Unknown protocol version");
            }

            FrameHeaderFlags flags = (FrameHeaderFlags) stream.ReadByte();

            byte streamId = (byte) stream.ReadByte();
            return streamId;
        }

        private void ThrowError()
        {
            ErrorCodes code = (ErrorCodes) _ms.ReadInt();
            string msg = _ms.ReadString();

            switch (code)
            {
                case ErrorCodes.Unavailable:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) ReadShort();
                        int required = ReadInt();
                        int alive = ReadInt();
                        throw new UnavailableException(msg, cl, required, alive);
                    }

                case ErrorCodes.WriteTimeout:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) ReadShort();
                        int received = ReadInt();
                        int blockFor = ReadInt();
                        string writeType = ReadString();
                        throw new WriteTimeOutException(msg, cl, received, blockFor, writeType);
                    }

                case ErrorCodes.ReadTimeout:
                    {
                        ConsistencyLevel cl = (ConsistencyLevel) ReadShort();
                        int received = ReadInt();
                        int blockFor = ReadInt();
                        bool dataPresent = 0 != ReadByte();
                        throw new ReadTimeOutException(msg, cl, received, blockFor, dataPresent);
                    }

                case ErrorCodes.Syntax:
                    throw new SyntaxException(msg);

                case ErrorCodes.Unauthorized:
                    throw new UnauthorizedException(msg);

                case ErrorCodes.Invalid:
                    throw new InvalidException(msg);

                case ErrorCodes.AlreadyExists:
                    {
                        string keyspace = ReadString();
                        string table = ReadString();
                        throw new AlreadyExistsException(msg, keyspace, table);
                    }

                case ErrorCodes.Unprepared:
                    {
                        byte[] unknownId = ReadShortBytes();
                        throw new UnpreparedException(msg, unknownId);
                    }

                default:
                    throw new CassandraException(code, msg);
            }
        }
    }
}