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

namespace CassandraSharp.Core.Utils.Stream
{
    internal sealed class DebugStream : System.IO.Stream
    {
        private OperationState _opeState = OperationState.Unknown;

        private System.IO.Stream _stream;

        public DebugStream(System.IO.Stream stream)
        {
            _stream = stream;
        }

        public override bool CanRead
        {
            get { return _stream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return _stream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return _stream.CanWrite; }
        }

        public override long Length
        {
            get { return _stream.Length; }
        }

        public override long Position
        {
            get { return _stream.Position; }
            set { _stream.Position = value; }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                _stream.Dispose();
                _stream = null;
            }
        }

        public override void Flush()
        {
            Console.Write("*");
            _stream.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _stream.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = _stream.Read(buffer, offset, count);

            if (_opeState != OperationState.Read)
            {
                _opeState = OperationState.Read;
                Console.WriteLine();
                Console.Write("READ: ");
            }

            for (int i = 0; i < read; ++i)
            {
                Console.Write("{0:X2}", buffer[i]);
            }

            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (_opeState != OperationState.Write)
            {
                _opeState = OperationState.Write;
                Console.WriteLine();
                Console.Write("WRITE: ");
            }

            for (int i = 0; i < count; ++i)
            {
                Console.Write("{0:X2}", buffer[offset + i]);
            }

            _stream.Write(buffer, offset, count);
        }

        private enum OperationState
        {
            Unknown,

            Read,

            Write
        }
    }
}