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
    using System.IO;
    using System.Net.Sockets;
    using CassandraSharp.Utils;

    internal class BufferingFrameReader : StreamingFrameReader
    {
        private readonly byte[] _buffer;

        private readonly Stream _ms;

        public BufferingFrameReader(Socket socket)
                : base(socket)
        {
            _buffer = new byte[FrameBytesLeft];
            base.ReceiveBuffer(_buffer, 0, _buffer.Length);
            _ms = new MemoryStream(_buffer);
        }

        public override void Dispose()
        {
            _ms.SafeDispose();
            base.Dispose();
        }

        protected override void ReceiveBuffer(byte[] buffer, int offset, int len)
        {
            _ms.Read(buffer, offset, len);
        }
    }
}