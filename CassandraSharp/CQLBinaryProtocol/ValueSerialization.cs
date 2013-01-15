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
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Text;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal static class ValueSerialization
    {
        private static readonly Dictionary<ColumnType, Type> _colType2Type = new Dictionary<ColumnType, Type>
            {
                    {ColumnType.Ascii, typeof(string)},
                    {ColumnType.Text, typeof(string)},
                    {ColumnType.Varchar, typeof(string)},
                    {ColumnType.Blob, typeof(byte[])},
                    {ColumnType.Double, typeof(double)},
                    {ColumnType.Float, typeof(float)},
                    {ColumnType.Bigint, typeof(long)},
                    {ColumnType.Counter, typeof(long)},
                    {ColumnType.Int, typeof(int)},
                    {ColumnType.Boolean, typeof(bool)},
                    {ColumnType.Uuid, typeof(Guid)},
                    {ColumnType.Timeuuid, typeof(Guid)},
                    {ColumnType.Inet, typeof(IPAddress)},
            };

        public static byte[] Serialize(this IColumnSpec columnSpec, object data)
        {
            byte[] rawData;
            switch (columnSpec.ColumnType)
            {
                case ColumnType.List:
                case ColumnType.Set:
                    ICollection coll = (ICollection) data;
                    using (MemoryStream ms = new MemoryStream())
                    {
                        ms.WriteShort((short) coll.Count);
                        foreach (object elem in coll)
                        {
                            byte[] rawDataElem = Serialize(columnSpec.CollectionValueType, elem);
                            ms.WriteShortByteArray(rawDataElem);
                        }
                        rawData = ms.ToArray();
                    }
                    break;

                case ColumnType.Map:
                    IDictionary map = (IDictionary) data;
                    using (MemoryStream ms = new MemoryStream())
                    {
                        ms.WriteShort((short) map.Count);
                        foreach (DictionaryEntry de in map)
                        {
                            byte[] rawDataKey = Serialize(columnSpec.CollectionKeyType, de.Key);
                            ms.WriteShortByteArray(rawDataKey);
                            byte[] rawDataValue = Serialize(columnSpec.CollectionValueType, de.Value);
                            ms.WriteShortByteArray(rawDataValue);
                        }
                        rawData = ms.ToArray();
                    }
                    break;

                default:
                    rawData = Serialize(columnSpec.ColumnType, data);
                    break;
            }

            return rawData;
        }

        private static byte[] Serialize(ColumnType colType, object data)
        {
            byte[] rawData;
            switch (colType)
            {
                case ColumnType.Ascii:
                case ColumnType.Text:
                case ColumnType.Varchar:
                    rawData = Encoding.ASCII.GetBytes((string) data);
                    break;

                case ColumnType.Blob:
                    rawData = (byte[]) data;
                    break;

                case ColumnType.Double:
                    rawData = BitConverter.GetBytes((double) data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Float:
                    rawData = BitConverter.GetBytes((float) data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Timestamp:
                case ColumnType.Bigint:
                case ColumnType.Counter:
                    rawData = BitConverter.GetBytes((long) data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Int:
                    rawData = BitConverter.GetBytes((int) data);
                    rawData.ReverseIfLittleEndian();
                    break;

                case ColumnType.Boolean:
                    rawData = BitConverter.GetBytes((bool) data);
                    break;

                case ColumnType.Uuid:
                case ColumnType.Timeuuid:
                    rawData = ((Guid)data).ToByteArray();
                    rawData.ReverseIfLittleEndian(0, 4);
                    rawData.ReverseIfLittleEndian(4, 2);
                    rawData.ReverseIfLittleEndian(6, 2);
                    break;

                case ColumnType.Inet:
                    rawData = ((IPAddress) data).GetAddressBytes();
                    break;

                default:
                    throw new ArgumentException("Unsupported type");
            }

            return rawData;
        }

        public static object Deserialize(this IColumnSpec columnSpec, byte[] rawData)
        {
            object data;
            Type colType;
            switch (columnSpec.ColumnType)
            {
                default:
                    data = Deserialize(columnSpec.ColumnType, rawData);
                    break;

                case ColumnType.List:
                    colType = columnSpec.CollectionValueType.ToType();
                    Type typedColl = typeof(ListInitializer<>).MakeGenericType(colType);
                    ICollectionInitializer list = (ICollectionInitializer) Activator.CreateInstance(typedColl);
                    using (MemoryStream ms = new MemoryStream(rawData))
                    {
                        short nbElem = ms.ReadShort();
                        while (0 < nbElem)
                        {
                            byte[] elemRawData = ms.ReadShortBytes();
                            object elem = Deserialize(columnSpec.CollectionValueType, elemRawData);
                            list.Add(elem);
                            --nbElem;
                        }
                        data = list.Collection;
                    }
                    break;

                case ColumnType.Map:
                    Type keyType = columnSpec.CollectionKeyType.ToType();
                    colType = columnSpec.CollectionValueType.ToType();
                    Type typedDic = typeof(DictionaryInitializer<,>).MakeGenericType(keyType, colType);
                    IDictionaryInitializer dic = (IDictionaryInitializer) Activator.CreateInstance(typedDic);
                    using (MemoryStream ms = new MemoryStream(rawData))
                    {
                        short nbElem = ms.ReadShort();
                        while (0 < nbElem)
                        {
                            byte[] elemRawKey = ms.ReadShortBytes();
                            byte[] elemRawValue = ms.ReadShortBytes();
                            object key = Deserialize(columnSpec.CollectionValueType, elemRawKey);
                            object value = Deserialize(columnSpec.CollectionValueType, elemRawValue);
                            dic.Add(key, value);
                            --nbElem;
                        }
                        data = dic.Collection;
                    }
                    break;

                case ColumnType.Set:
                    colType = columnSpec.CollectionValueType.ToType();
                    Type typedSet = typeof(HashSetInitializer<>).MakeGenericType(colType);
                    ICollectionInitializer set = (ICollectionInitializer) Activator.CreateInstance(typedSet);
                    using (MemoryStream ms = new MemoryStream(rawData))
                    {
                        short nbElem = ms.ReadShort();
                        while (0 < nbElem)
                        {
                            byte[] elemRawData = ms.ReadShortBytes();
                            object elem = Deserialize(columnSpec.CollectionValueType, elemRawData);
                            set.Add(elem);
                            --nbElem;
                        }
                        data = set.Collection;
                    }
                    break;
            }

            return data;
        }

        private static object Deserialize(ColumnType colType, byte[] rawData)
        {
            object data;
            switch (colType)
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

                case ColumnType.Timestamp:
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
                    rawData.ReverseIfLittleEndian(0, 4);
                    rawData.ReverseIfLittleEndian(4, 2);
                    rawData.ReverseIfLittleEndian(6, 2);
                    data = new Guid(rawData);
                    break;

                case ColumnType.Inet:
                    data = new IPAddress(rawData);
                    break;

                default:
                    throw new ArgumentException("Unsupported type");
            }

            return data;
        }

        private static Type ToType(this ColumnType colType)
        {
            Type type;
            if (_colType2Type.TryGetValue(colType, out type))
            {
                return type;
            }

            throw new ArgumentException("Unsupported type");
        }
    }
}