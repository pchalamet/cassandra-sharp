// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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
using System.Collections.Generic;
using System.Text;

namespace CassandraSharp.Utils.Stream
{
    internal static class BigEndianStreamExtensions
    {
        public static void WriteUShort(this System.IO.Stream stream, ushort data)
        {
            var buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            stream.Write(buffer, 0, buffer.Length);
        }

        public static void WriteInt(this System.IO.Stream stream, int data)
        {
            var buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            stream.Write(buffer, 0, buffer.Length);
        }

        public static void WriteString(this System.IO.Stream stream, string data)
        {
            var bufStr = Encoding.UTF8.GetBytes(data);
            var len = (ushort)bufStr.Length;
            stream.WriteUShort(len);
            stream.Write(bufStr, 0, len);
        }

        public static void WriteStringList(this System.IO.Stream stream, string[] data)
        {
            stream.WriteUShort((ushort)data.Length);
            foreach (var s in data) stream.WriteString(s);
        }

        public static void WriteLongString(this System.IO.Stream stream, string data)
        {
            var bufStr = Encoding.UTF8.GetBytes(data);
            var len = bufStr.Length;
            stream.WriteInt(len);
            stream.Write(bufStr, 0, len);
        }

        public static void WriteStringMap(this System.IO.Stream stream, Dictionary<string, string> dic)
        {
            stream.WriteUShort((ushort)dic.Count);
            foreach (var kvp in dic)
            {
                stream.WriteString(kvp.Key);
                stream.WriteString(kvp.Value);
            }
        }

        public static void WriteShortBytes(this System.IO.Stream stream, byte[] data)
        {
            var len = (ushort)data.Length;
            stream.WriteUShort(len);
            stream.Write(data, 0, len);
        }

        public static void WriteBytesArray(this System.IO.Stream stream, byte[] data)
        {
            if (null != data)
            {
                var len = data.Length;
                stream.WriteInt(len);
                stream.Write(data, 0, len);
            }
            else
            {
                stream.WriteInt(-1);
            }
        }

        private static void ReadBuffer(this System.IO.Stream stream, byte[] buffer)
        {
            var read = 0;
            var len = buffer.Length;
            while (read != len) read += stream.Read(buffer, read, len - read);
        }

        public static ushort ReadUShort(this System.IO.Stream stream)
        {
            var buffer = new byte[2];
            stream.ReadBuffer(buffer);
            buffer.ReverseIfLittleEndian();

            var data = BitConverter.ToUInt16(buffer, 0);
            return data;
        }

        public static int ReadInt(this System.IO.Stream stream)
        {
            var buffer = new byte[4];
            stream.ReadBuffer(buffer);
            buffer.ReverseIfLittleEndian();

            var data = BitConverter.ToInt32(buffer, 0);
            return data;
        }

        public static string ReadString(this System.IO.Stream stream)
        {
            var len = stream.ReadUShort();
            if (0 != len)
            {
                var bufStr = new byte[len];
                stream.ReadBuffer(bufStr);
                var data = Encoding.UTF8.GetString(bufStr);
                return data;
            }

            return string.Empty;
        }

        public static byte[] ReadBytesArray(this System.IO.Stream stream)
        {
            var len = stream.ReadInt();
            if (-1 != len)
            {
                var data = new byte[len];
                stream.ReadBuffer(data);
                return data;
            }

            return null;
        }

        public static byte[] ReadShortBytes(this System.IO.Stream stream)
        {
            var len = stream.ReadUShort();
            var data = new byte[len];
            stream.ReadBuffer(data);
            return data;
        }

        public static string[] ReadStringList(this System.IO.Stream stream)
        {
            var len = stream.ReadUShort();
            var data = new string[len];
            for (var i = 0; i < len; ++i) data[i] = stream.ReadString();
            return data;
        }

        public static Dictionary<string, string[]> ReadStringMultimap(this System.IO.Stream stream)
        {
            var len = stream.ReadUShort();
            var data = new Dictionary<string, string[]>(len);
            for (var i = 0; i < len; ++i)
            {
                var key = stream.ReadString();
                var value = stream.ReadStringList();
                data.Add(key, value);
            }

            return data;
        }
    }
}