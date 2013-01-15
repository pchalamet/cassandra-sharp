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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    internal static class BigEndianStreamExtensions
    {
        public static void WriteShort(this Stream stream, short data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            stream.Write(buffer, 0, buffer.Length);
        }

        public static void WriteInt(this Stream stream, int data)
        {
            byte[] buffer = BitConverter.GetBytes(data);
            buffer.ReverseIfLittleEndian();
            stream.Write(buffer, 0, buffer.Length);
        }

        public static void WriteString(this Stream stream, string data)
        {
            byte[] bufStr = Encoding.UTF8.GetBytes(data);
            short len = (short)bufStr.Length;
            stream.WriteShort(len);
            stream.Write(bufStr, 0, len);
        }

        public static void WriteStringList(this Stream stream, string[] data)
        {
            stream.WriteShort((short) data.Length);
            foreach (string s in data)
            {
                stream.WriteString(s);
            }
        }

        public static void WriteLongString(this Stream stream, string data)
        {
            byte[] bufStr = Encoding.UTF8.GetBytes(data);
            int len = bufStr.Length;
            stream.WriteInt(len);
            stream.Write(bufStr, 0, len);
        }

        public static void WriteStringMap(this Stream stream, Dictionary<string, string> dic)
        {
            stream.WriteShort((short) dic.Count);
            foreach (var kvp in dic)
            {
                stream.WriteString(kvp.Key);
                stream.WriteString(kvp.Value);
            }
        }

        public static void WriteShortByteArray(this Stream stream, byte[] data)
        {
            short len = (short) data.Length;
            stream.WriteShort(len);
            stream.Write(data, 0, len);
        }

        public static void WriteByteArray(this Stream stream, byte[] data)
        {
            int len = data.Length;
            stream.WriteInt(len);
            stream.Write(data, 0, len);
        }

        public static short ReadShort(this Stream stream)
        {
            byte[] buffer = new byte[2];
            stream.Read(buffer, 0, buffer.Length);
            buffer.ReverseIfLittleEndian();

            short data = BitConverter.ToInt16(buffer, 0);
            return data;
        }

        public static int ReadInt(this Stream stream)
        {
            byte[] buffer = new byte[4];
            stream.Read(buffer, 0, buffer.Length);
            buffer.ReverseIfLittleEndian();

            int data = BitConverter.ToInt32(buffer, 0);
            return data;
        }

        public static string ReadString(this Stream stream)
        {
            short len = stream.ReadShort();
            if (0 == len)
            {
                return string.Empty;
            }

            byte[] bufStr = new byte[len];
            stream.Read(bufStr, 0, len);
            string data = Encoding.UTF8.GetString(bufStr);
            return data;
        }

        public static byte[] ReadBytes(this Stream stream)
        {
            int len = stream.ReadInt();
            if (-1 == len)
            {
                return null;
            }

            byte[] data = new byte[len];
            if (0 < len)
            {
                stream.Read(data, 0, len);
            }

            return data;
        }

        public static byte[] ReadShortBytes(this Stream stream)
        {
            short len = stream.ReadShort();
            byte[] data = new byte[len];
            if (0 < len)
            {
                stream.Read(data, 0, len);
            }

            return data;
        }

        public static string[] ReadStringList(this Stream stream)
        {
            short len = stream.ReadShort();
            string[] data = new string[len];
            for (int i = 0; i < len; ++i)
            {
                data[i] = stream.ReadString();
            }
            return data;
        }

        public static Dictionary<string, string[]> ReadStringMultimap(this Stream stream)
        {
            short len = stream.ReadShort();
            Dictionary<string, string[]> data = new Dictionary<string, string[]>(len);
            for (int i = 0; i < len; ++i)
            {
                string key = stream.ReadString();
                string[] value = stream.ReadStringList();
                data.Add(key, value);
            }

            return data;
        }
    }
}