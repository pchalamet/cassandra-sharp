// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
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
    using System.Net;
    using System.Text;

    internal static class PrimitiveOnWireExtensions
    {
        public static byte[] GetBytes(this string data)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(data);
            return buffer;
        }

        public static byte[] GetBytes(this double data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            return buffer;
        }

        public static byte[] GetBytes(this float data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            return buffer;
        }

        public static byte[] GetBytes(this bool data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            return buffer;
        }

        public static byte[] GetBytes(this DateTime data)
        {
            byte[] buffer = BitConverter.GetBytes(data.ToTimestamp());
            buffer.ReverseIfLittleEndian();
            return buffer;
        }

        public static byte[] GetBytes(this Guid data)
        {
            byte[] buffer = data.ToByteArray();
            buffer.ReverseIfLittleEndian(0, 4);
            buffer.ReverseIfLittleEndian(4, 2);
            buffer.ReverseIfLittleEndian(6, 2);
            return buffer;
        }

        public static byte[] GetBytes(this IPAddress data)
        {
            byte[] buffer = data.GetAddressBytes();
            return buffer;
        }

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

        public static string ToString(this byte[] buffer)
        {
            string data = Encoding.UTF8.GetString(buffer);
            return data;
        }

        public static double ToDouble(this byte[] buffer)
        {
            buffer.ReverseIfLittleEndian();
            double data = BitConverter.ToDouble(buffer, 0);
            return data;
        }

        public static float ToFloat(this byte[] buffer)
        {
            buffer.ReverseIfLittleEndian();
            float data = BitConverter.ToSingle(buffer, 0);
            return data;
        }

        public static bool ToBoolean(this byte[] buffer)
        {
            bool data = BitConverter.ToBoolean(buffer, 0);
            return data;
        }

        public static DateTime ToDateTime(this byte[] buffer)
        {
            buffer.ReverseIfLittleEndian();
            DateTime data = BitConverter.ToInt64(buffer, 0).ToDateTime();
            return data;
        }

        public static Guid ToGuid(this byte[] buffer)
        {
            buffer.ReverseIfLittleEndian(0, 4);
            buffer.ReverseIfLittleEndian(4, 2);
            buffer.ReverseIfLittleEndian(6, 2);
            Guid guid = new Guid(buffer);
            return guid;
        }

        public static IPAddress ToIPAddress(this byte[] buffer)
        {
            IPAddress data = new IPAddress(buffer);
            return data;
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
    }
}