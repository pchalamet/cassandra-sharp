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

namespace CassandraSharp.Utils.Stream
{
    using System;
    using System.IO;
    using System.Net.Sockets;

    internal sealed class SocketReadOnlyStream : Stream
    {
        private readonly Socket _socket;

        private int _len;

        public SocketReadOnlyStream(Socket socket, int len)
        {
            _socket = socket;
            _len = len;
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                byte[] buffer = new byte[32];

                // skip unread data
                while (0 < _len)
                {
                    int left = Math.Min(_len, buffer.Length);
                    _len -= SocketReceiveBuffer(_socket, buffer, 0, left);
                }
            }
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            _len -= count;
            if (_len < 0)
            {
                throw new IOException("Attempt to read past frame");
            }

            int read = SocketReceiveBuffer(_socket, buffer, offset, count);

            if (read != count)
            {
                throw new IOException("Failed to read requested count");
            }

            return read;
        }

        public static int SocketReceiveBuffer(Socket socket, byte[] buffer, int offset, int len)
        {
            int read = 0;
            while (read != len)
            {
                read += socket.Receive(buffer, offset + read, len - read, SocketFlags.None);
            }

            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}