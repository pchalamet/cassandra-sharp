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
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal class FrameWriter : IFrameWriter, IDisposable
    {
        private readonly Stream _stream;

        private readonly byte _streamId;

        private readonly bool _tracing;

        private readonly MemoryStream _ms;

        internal FrameWriter(Stream stream, byte streamId, bool tracing)
        {
            _stream = stream;
            _streamId = streamId;
            _tracing = tracing;
            _ms = new MemoryStream();
        }

        public void Dispose()
        {
            _stream.Flush();
            _ms.SafeDispose();
        }

        public void Send(MessageOpcodes msgOpcode)
        {
            const byte version = (byte) (FrameType.Request | FrameType.ProtocolVersion);
            _stream.WriteByte(version);

            FrameHeaderFlags flags = FrameHeaderFlags.None;
            if (_tracing)
            {
                flags |= FrameHeaderFlags.Tracing;
            }

            //if (compress)
            //{
            //    flags |= 0x01;
            //}
            _stream.WriteByte((byte)flags);

            // streamId
            _stream.WriteByte(_streamId);

            // opcode
            _stream.WriteByte((byte)msgOpcode);

            // len of body
            int bodyLen = (int) _ms.Length;
            _stream.WriteInt(bodyLen);

            // body
            _stream.Write(_ms.GetBuffer(), 0, bodyLen);
            _stream.Flush();
        }

        public void WriteShort(short data)
        {
            _ms.WriteShort(data);
        }

        public void WriteInt(int data)
        {
            _ms.WriteInt(data);
        }

        public void WriteString(string data)
        {
            _ms.WriteString(data);
        }

        public void WriteShortByteArray(byte[] data)
        {
            _ms.WriteShortByteArray(data);
        }

        public void WriteLongString(string data)
        {
            _ms.WriteLongString(data);
        }

        public void WriteStringMap(Dictionary<string, string> dic)
        {
            _ms.WriteStringMap(dic);
        }

        public void WriteStringList(string[] data)
        {
            _ms.WriteStringList(data);
        }

        public void WriteByteArray(byte[] data)
        {
            _ms.WriteByteArray(data);
        }
    }
}