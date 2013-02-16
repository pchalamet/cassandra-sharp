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

namespace CassandraSharp.Transport.Stream
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net.Sockets;
    using System.Text;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal class BufferingFrameWriter : IFrameWriter,
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

        public void SetMessageType(MessageOpcodes msgOpcode)
        {
            _msgOpcode = msgOpcode;
        }

        public void WriteShort(short data)
        {
            byte[] buffer = data.GetBytes();
            _ms.Write(buffer, 0, buffer.Length);
        }

        public void WriteInt(int data)
        {
            byte[] buffer = data.GetBytes();
            _ms.Write(buffer, 0, buffer.Length);
        }

        public void WriteString(string data)
        {
            byte[] bufStr = Encoding.UTF8.GetBytes(data);
            short len = (short) bufStr.Length;
            byte[] bufLen = len.GetBytes();
            _ms.Write(bufLen, 0, bufLen.Length);
            _ms.Write(bufStr, 0, bufStr.Length);
        }

        public void WriteShortByteArray(byte[] data)
        {
            short len = (short) data.Length;
            byte[] bufLen = len.GetBytes();
            _ms.Write(bufLen, 0, bufLen.Length);
            _ms.Write(data, 0, data.Length);
        }

        public void WriteLongString(string data)
        {
            byte[] bufStr = Encoding.UTF8.GetBytes(data);
            int len = bufStr.Length;
            byte[] bufLen = len.GetBytes();
            _ms.Write(bufLen, 0, bufLen.Length);
            _ms.Write(bufStr, 0, bufStr.Length);
        }

        public void WriteStringMap(Dictionary<string, string> dic)
        {
            short len = (short) dic.Count;
            byte[] bufLen = len.GetBytes();
            _ms.Write(bufLen, 0, bufLen.Length);
            foreach (var kvp in dic)
            {
                WriteString(kvp.Key);
                WriteString(kvp.Value);
            }
        }

        public void WriteStringList(string[] data)
        {
            short len = (short) data.Length;
            byte[] bufLen = len.GetBytes();
            _ms.Write(bufLen, 0, bufLen.Length);
            foreach (string s in data)
            {
                WriteString(s);
            }
        }

        public void WriteByteArray(byte[] data)
        {
            int len = data.Length;
            byte[] bufLen = len.GetBytes();
            _ms.Write(bufLen, 0, bufLen.Length);
            _ms.Write(data, 0, data.Length);
        }

        public void SendFrame(byte streamId, Socket socket)
        {
            const byte version = (byte) (FrameType.Request | FrameType.ProtocolVersion);
            FrameHeaderFlags flags = FrameHeaderFlags.None;
            if (_tracing)
            {
                flags |= FrameHeaderFlags.Tracing;
            }

            byte[] header = new byte[4];
            header[0] = version;
            header[1] = (byte) flags;
            header[2] = streamId;
            header[3] = (byte) _msgOpcode;
            SendBuffer(socket, header);

            // len of body
            int len = (int) _ms.Length;
            byte[] bodyLen = len.GetBytes();
            SendBuffer(socket, bodyLen);

            // body
            SendBuffer(socket, _ms.GetBuffer(), 0, len);
        }

        private void SendBuffer(Socket socket, byte[] buffer)
        {
            SendBuffer(socket, buffer, 0, buffer.Length);
        }

        private static void SendBuffer(Socket socket, byte[] buffer, int offset, int len)
        {
            int written = 0;
            while (written != len)
            {
                written += socket.Send(buffer, offset + written, len - written, SocketFlags.None);
            }
        }
    }
}