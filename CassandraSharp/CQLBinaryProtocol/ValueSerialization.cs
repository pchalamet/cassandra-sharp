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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Linq;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using CassandraSharp.Utils.Stream;

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
                    var colType = columnSpec.CollectionValueType.ToType();
                    Type typedColl = typeof(CollectionAccessor<>).MakeGenericType(colType);
                    ICollectionAccessor coll = (ICollectionAccessor) Activator.CreateInstance(typedColl, data);
                    using (MemoryStream ms = new MemoryStream())
                    {
                        ms.WriteUShort((ushort) coll.Count);
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
                        ms.WriteUShort((ushort) map.Count);
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
                    rawData = ((string) data).GetBytes();
                    break;

                case ColumnType.Blob:
                    rawData = (byte[]) data;
                    break;

                case ColumnType.Double:
                    rawData = ((double) data).GetBytes();
                    break;

                case ColumnType.Float:
                    rawData = ((float) data).GetBytes();
                    break;

                case ColumnType.Timestamp:
                    rawData = ((DateTime) data).GetBytes();
                    break;

                case ColumnType.Bigint:
                case ColumnType.Counter:
                    rawData = ((long) data).GetBytes();
                    break;

                case ColumnType.Int:
                    rawData = ((int) data).GetBytes();
                    break;

                case ColumnType.Boolean:
                    rawData = ((bool) data).GetBytes();
                    break;

                case ColumnType.Uuid:
                case ColumnType.Timeuuid:
                    rawData = ((Guid) data).GetBytes();
                    break;

                case ColumnType.Inet:
                    rawData = ((IPAddress) data).GetBytes();
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
                        ushort nbElem = ms.ReadUShort();
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
                        ushort nbElem = ms.ReadUShort();
                        while (0 < nbElem)
                        {
                            byte[] elemRawKey = ms.ReadShortBytes();
                            byte[] elemRawValue = ms.ReadShortBytes();
                            object key = Deserialize(columnSpec.CollectionKeyType, elemRawKey);
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
                        ushort nbElem = ms.ReadUShort();
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
                    data = rawData.ToDouble();
                    break;

                case ColumnType.Float:
                    data = rawData.ToFloat();
                    break;

                case ColumnType.Timestamp:
                    data = rawData.ToDateTime();
                    break;

                case ColumnType.Bigint:
                case ColumnType.Counter:
                    data = rawData.ToLong(0);
                    break;

                case ColumnType.Int:
                    data = rawData.ToInt(0);
                    break;

                case ColumnType.Boolean:
                    data = rawData.ToBoolean();
                    break;

                case ColumnType.Uuid:
                case ColumnType.Timeuuid:
                    data = rawData.ToGuid();
                    break;

                case ColumnType.Inet:
                    data = rawData.ToIPAddress();
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



        /// <summary>
        /// Gets murmur hash based on a provided value.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static long getMurmur3Hash(ColumnType type, Object value)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            byte[] partitionKey = Serialize(type, value);
            return MurmurHash.Hash3_x64_128(partitionKey, 0, partitionKey.Length, 0)[0];
        }

        /// <summary>
        /// Gets murmur hash based on the provided values. Use this when composite partition keys are used
        /// </summary>
        /// <param name="types"></param>
        /// <param name="values">The values must be given in the same order as the partition key is defined.</param>
        /// <returns></returns>
        public static long getCompositeMurmur3Hash(ColumnType[] types, Object[] values)
        {
            if (types == null)
                throw new ArgumentNullException("types");

            if (values == null)
                throw new ArgumentNullException("values");


            if (types.Length != values.Length)
                throw new ArgumentException("types and values are not of equal length");

            var rawValues = new byte[types.Length][];
            for (int i = 0; i < types.Length; i++)
            {
                rawValues[i] = Serialize(types[i], values[i]);
            }

            int length = types.Length * 3 + rawValues.Sum(val => val.Length);
            using (var stream = new MemoryStream(length))
            {
                foreach (var rawValue in rawValues)
                {
                    //write length of composite key part as short
                    var len = (short)rawValue.Length;
                    stream.WriteByte((byte)(len >> 8));
                    stream.WriteByte((byte)(len));

                    //write value
                    stream.Write(rawValue, 0, len);

                    //write terminator byte
                    stream.WriteByte(0);
                }

                byte[] partitionKey = stream.ToArray();
                return MurmurHash.Hash3_x64_128(partitionKey, 0, partitionKey.Length, 0)[0];
            }
        }
    }
}