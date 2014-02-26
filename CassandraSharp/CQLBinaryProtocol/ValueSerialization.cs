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
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using CassandraSharp.Utils.Collections;
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

        private static readonly Dictionary<Type, ColumnType> _type2ColType = new Dictionary<Type, ColumnType>
            {
                    {typeof(string), ColumnType.Varchar},
                    {typeof(byte[]), ColumnType.Blob},
                    {typeof(double), ColumnType.Double},
                    {typeof(float), ColumnType.Float},
                    {typeof(long), ColumnType.Bigint},
                    {typeof(int), ColumnType.Int},
                    {typeof(bool), ColumnType.Boolean},
                    {typeof(Guid), ColumnType.Uuid},
                    {typeof(IPAddress), ColumnType.Inet},
            };

        public static byte[] Serialize(this IColumnSpec columnSpec, object data)
        {
            byte[] rawData;
            switch (columnSpec.ColumnType)
            {
                case ColumnType.List:
                    return SerializeList((IList)data, value => Serialize(columnSpec.CollectionValueType, value));

                case ColumnType.Set:
                    var colType = columnSpec.CollectionValueType.ToType();

                    Type typedSet = typeof(HashSet<>).MakeGenericType(colType);
                    object hashSet = Activator.CreateInstance(typedSet);
                    Type accessorType = typeof(HashSetAccessor<>).MakeGenericType(colType);
                    var setAccessor = (IHashSetAccessor)Activator.CreateInstance(accessorType, hashSet);

                    return SerializeSet(setAccessor, value => Serialize(columnSpec.CollectionValueType, value));

                case ColumnType.Map:
                    return SerializeMap((IDictionary)data, key => Serialize(columnSpec.CollectionKeyType, key), value => Serialize(columnSpec.CollectionValueType, value));

                default:
                    rawData = Serialize(columnSpec.ColumnType, data);
                    break;
            }

            return rawData;
        }

        public static object DeserializeMap(byte[] rawData, IDictionary destMap, Func<byte[], object> keyDeserializer, Func<byte[], object> valueDeserializer)
        {
            using (MemoryStream ms = new MemoryStream(rawData))
            {
                ushort nbElem = ms.ReadUShort();
                while (0 < nbElem)
                {
                    byte[] elemRawKey = ms.ReadShortBytes();
                    byte[] elemRawValue = ms.ReadShortBytes();
                    object key = keyDeserializer(elemRawKey);
                    object value = valueDeserializer(elemRawValue);
                    destMap.Add(key, value);
                    --nbElem;
                }

                return destMap;
            }
        }

        public static object DeserializeList(byte[] rawData, IList destList, Func<byte[], object> valueDeserializer)
        {
            using (MemoryStream ms = new MemoryStream(rawData))
            {
                ushort nbElem = ms.ReadUShort();
                while (0 < nbElem)
                {
                    byte[] elemRawData = ms.ReadShortBytes();
                    object elem = valueDeserializer(elemRawData);
                    destList.Add(elem);
                    --nbElem;
                }

                return destList;
            }
        }

        public static object DeserializeSet(byte[] rawData, IHashSetAccessor destSet, Func<byte[], object> valueDeserializer)
        {
            using (MemoryStream ms = new MemoryStream(rawData))
            {
                ushort nbElem = ms.ReadUShort();
                while (0 < nbElem)
                {
                    byte[] elemRawData = ms.ReadShortBytes();
                    object elem = valueDeserializer(elemRawData);
                    destSet.AddItem(elem);
                    --nbElem;
                }

                return destSet;
            }
        }

        public static byte[] SerializeMap(IDictionary data, Func<object, byte[]> keySerializer, Func<object, byte[]> valueSerializer)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                ms.WriteUShort((ushort)data.Count);
                foreach (DictionaryEntry de in data)
                {
                    ms.WriteShortByteArray(keySerializer(de.Key));
                    ms.WriteShortByteArray(valueSerializer(de.Value));
                }

                return ms.ToArray();
            }
        }

        public static byte[] SerializeList(IList data, Func<object, byte[]> valueSerializer)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                ms.WriteUShort((ushort)data.Count);
                foreach (object elem in data)
                {
                    ms.WriteShortByteArray(valueSerializer(elem));
                }

                return ms.ToArray();
            }
        }

        public static byte[] SerializeSet(IHashSetAccessor data, Func<object, byte[]> valueSerializer)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                ms.WriteUShort((ushort)data.Count);
                foreach (object elem in data)
                {
                    ms.WriteShortByteArray(valueSerializer(elem));
                }

                return ms.ToArray();
            }
        }

        internal static byte[] Serialize(ColumnType colType, object data)
        {
            byte[] rawData;
            switch (colType)
            {
                case ColumnType.Ascii:
                case ColumnType.Text:
                case ColumnType.Varchar:
                    rawData = ((string)data).GetBytes();
                    break;

                case ColumnType.Blob:
                    rawData = (byte[])data;
                    break;

                case ColumnType.Double:
                    rawData = ((double)data).GetBytes();
                    break;

                case ColumnType.Float:
                    rawData = ((float)data).GetBytes();
                    break;

                case ColumnType.Timestamp:
                    rawData = ((DateTime)data).GetBytes();
                    break;

                case ColumnType.Bigint:
                case ColumnType.Counter:
                    rawData = ((long)data).GetBytes();
                    break;

                case ColumnType.Int:
                    rawData = ((int)data).GetBytes();
                    break;

                case ColumnType.Boolean:
                    rawData = ((bool)data).GetBytes();
                    break;

                case ColumnType.Uuid:
                case ColumnType.Timeuuid:
                    rawData = ((Guid)data).GetBytes();
                    break;

                case ColumnType.Inet:
                    rawData = ((IPAddress)data).GetBytes();
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

                    Type typedColl = typeof(List<>).MakeGenericType(columnSpec.CollectionValueType.ToType());
                    IList list = (IList)Activator.CreateInstance(typedColl);

                    return DeserializeList(rawData, list, value => Deserialize(columnSpec.CollectionValueType, value));

                case ColumnType.Map:
                    colType = columnSpec.CollectionValueType.ToType();
                    var colKeyType = columnSpec.CollectionKeyType.ToType();

                    Type typedDic = typeof(Dictionary<,>).MakeGenericType(colKeyType, colType);
                    IDictionary dic = (IDictionary)Activator.CreateInstance(typedDic);

                    return DeserializeMap(rawData, dic, key => Deserialize(columnSpec.CollectionKeyType, key), val => Deserialize(columnSpec.CollectionValueType, val));

                case ColumnType.Set:
                    colType = columnSpec.CollectionValueType.ToType();

                    Type typedSet = typeof(HashSet<>).MakeGenericType(colType);
                    object hashSet = Activator.CreateInstance(typedSet);

                    Type accessorType = typeof(HashSetAccessor<>).MakeGenericType(colType);
                    var setAccessor = (IHashSetAccessor)Activator.CreateInstance(accessorType, hashSet);
                    DeserializeSet(rawData, setAccessor, val => Deserialize(columnSpec.CollectionValueType, val));
                    return hashSet;
            }

            return data;
        }

        internal static object Deserialize(ColumnType colType, byte[] rawData)
        {
            object data;
            switch (colType)
            {
                case ColumnType.Ascii:
                case ColumnType.Text:
                case ColumnType.Varchar:
                    data = rawData.ToUtf8String();
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
                    throw new ArgumentException("Unsupported type: " + colType);
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

        public static ColumnType ToColumnType(this Type type)
        {
            ColumnType colType;
            if (_type2ColType.TryGetValue(type, out colType))
            {
                return colType;
            }

            throw new ArgumentException("Unsupported type");
        }
    }
}