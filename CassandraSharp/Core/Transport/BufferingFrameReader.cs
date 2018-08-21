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

using System.IO;
using System.Net.Sockets;

namespace CassandraSharp.Core.Transport
{
    internal class BufferingFrameReader : StreamingFrameReader
    {
        public BufferingFrameReader(Socket socket)
                : base(socket)
        {
        }

        protected override Stream CreateStream(Socket socket, int frameBytesLeft)
        {
            byte[] buffer = new byte[frameBytesLeft];
            int len = SocketReadOnlyStream.SocketReceiveBuffer(socket, buffer, 0, frameBytesLeft);
            if (len != frameBytesLeft)
            {
                throw new IOException("Failed to read required bytes for frame");
            }

            return new MemoryStream(buffer, false);
        }
    }
}