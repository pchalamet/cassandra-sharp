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
    using System.Net;
    using System.Text;
    using CassandraSharp.Extensibility;

    internal static class ColumnSpecExtensions
    {
        public static byte[] Serialize(this IColumnSpec columnSpec, object data)
        {
            byte[] rawData;
            switch (columnSpec.Type)
            {
                case ColumnType.Ascii:
                case ColumnType.Text:
                case ColumnType.Varchar:
                    rawData = Encoding.ASCII.GetBytes((string) data);
                    break;

                case ColumnType.Blob:
                    rawData = (byte[])data;
                    break;

                case ColumnType.Double:
                    rawData = BitConverter.GetBytes((double) data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Float:
                    rawData = BitConverter.GetBytes((float) data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Bigint:
                case ColumnType.Counter:
                    rawData = BitConverter.GetBytes((long)data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Int:
                    rawData = BitConverter.GetBytes((int) data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Boolean:
                    rawData = BitConverter.GetBytes((bool)data);
                    break;

                case ColumnType.Uuid:
                case ColumnType.Timeuuid:
                    rawData = ((Guid) data).ToByteArray();
                    break;

                case ColumnType.Inet:
                    rawData = ((IPAddress) data).GetAddressBytes();
                    break;

                case ColumnType.Custom:
                case ColumnType.Decimal:
                case ColumnType.Varint:
                case ColumnType.List:
                case ColumnType.Map:
                case ColumnType.Set:
                default:
                    throw new ArgumentException("Unsupported type");
            }

            return rawData;
        }

        public static object Deserialize(this IColumnSpec columnSpec, byte[] rawData)
        {
            object data;
            switch (columnSpec.Type)
            {
                case ColumnType.Ascii:
                case ColumnType.Text:
                case ColumnType.Varchar:
                    data = Encoding.ASCII.GetString(rawData);
                    break;

                case ColumnType.Blob:
                    data = rawData;
                    break;

                case ColumnType.Double:
                    rawData.ReverseIfLittleEndian();
                    data = BitConverter.ToDouble(rawData, 0);
                    break;

                case ColumnType.Float:
                    rawData.ReverseIfLittleEndian();
                    data = BitConverter.ToSingle(rawData, 0);
                    break;

                case ColumnType.Bigint:
                case ColumnType.Counter:
                    rawData.ReverseIfLittleEndian();
                    data = BitConverter.ToInt64(rawData, 0);
                    break;

                case ColumnType.Int:
                    rawData.ReverseIfLittleEndian();
                    data = BitConverter.ToInt32(rawData, 0);
                    break;

                case ColumnType.Boolean:
                    data = BitConverter.ToBoolean(rawData, 0);
                    break;

                case ColumnType.Uuid:
                case ColumnType.Timeuuid:
                    data = new Guid(rawData);
                    break;

                case ColumnType.Inet:
                    data = new IPAddress(rawData);
                    break;

                case ColumnType.Custom:
                case ColumnType.Decimal:
                case ColumnType.Varint:
                case ColumnType.List:
                case ColumnType.Map:
                case ColumnType.Set:
                default:
                    throw new ArgumentException("Unsupported type");
            }

            return data;
        }
    }
}