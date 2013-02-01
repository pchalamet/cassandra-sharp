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
    using System;
    using CassandraSharp.CQLBinaryProtocol;

    internal static class PrimitiveExtensions
    {
        public static byte[] GetBytes(this short data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            return buffer;
        }

        public static byte[] GetBytes(this int data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            return buffer;
        }

        public static byte[] GetBytes(this long data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            return buffer;
        }

        public static short ToShort(this byte[] buffer, int offset)
        {
            buffer.ReverseIfLittleEndian(offset, sizeof (short));
            return BitConverter.ToInt16(buffer, offset);
        }

        public static int ToInt(this byte[] buffer, int offset)
        {
            buffer.ReverseIfLittleEndian(offset, sizeof (int));
            return BitConverter.ToInt32(buffer, offset);
        }

        public static long ToLong(this byte[] buffer, int offset)
        {
            buffer.ReverseIfLittleEndian(offset, sizeof (long));
            return BitConverter.ToInt64(buffer, offset);
        }

        public static Guid ToGuid(this byte[] buffer)
        {
            buffer.ReverseIfLittleEndian(0, 4);
            buffer.ReverseIfLittleEndian(4, 2);
            buffer.ReverseIfLittleEndian(6, 2);
            Guid guid = new Guid(buffer);
            return guid;
        }
    }
}