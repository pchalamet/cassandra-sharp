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

using System;
using System.IO;
using System.Net.Sockets;
using CassandraSharp.CQLBinaryProtocol;
using CassandraSharp.Extensibility;
using CassandraSharp.Utils;
using CassandraSharp.Utils.Stream;

namespace CassandraSharp.Transport
{
    internal sealed class BufferingFrameWriter : IFrameWriter,
                                                 IDisposable
    {
        private readonly MemoryStream _ms;

        private readonly bool _tracing;

        private MessageOpcodes _msgOpcode = MessageOpcodes.Error;

        internal BufferingFrameWriter(bool tracing)
        {
            _tracing = tracing;
            _ms = new MemoryStream();
        }

        public void Dispose()
        {
            _ms.SafeDispose();
        }

        public Stream WriteOnlyStream => _ms;

        public void SetMessageType(MessageOpcodes msgOpcode)
        {
            _msgOpcode = msgOpcode;
        }

        public void SendFrame(ushort streamId, Socket socket)
        {
            const byte version = (byte)(FrameType.Request | FrameType.ProtocolVersion);
            var flags = FrameHeaderFlags.None;
            if (_tracing) flags |= FrameHeaderFlags.Tracing;

            var header = new byte[5];
            header[0] = version;
            header[1] = (byte)flags;
            header[2] = (byte)(streamId >> 8);
            header[3] = (byte)streamId;
            header[4] = (byte)_msgOpcode;
            SendBuffer(socket, header);

            var len = (int)_ms.Length;
            var bodyLen = len.GetBytes();
            SendBuffer(socket, bodyLen);

            // body
            SendBuffer(socket, _ms.GetBuffer(), 0, len);
        }

        private static void SendBuffer(Socket socket, byte[] buffer)
        {
            SendBuffer(socket, buffer, 0, buffer.Length);
        }

        private static void SendBuffer(Socket socket, byte[] buffer, int offset, int len)
        {
            var written = 0;
            while (written != len) written += socket.Send(buffer, offset + written, len - written, SocketFlags.None);
        }
    }
}