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
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Utils.Collections;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;

    public interface IValueSerializer
    {
        byte[] Serialize(object value);

        object Deserialize(byte[] rawData);
    }

    public class ValueSerializer<T> : IValueSerializer
    {
        private readonly Func<object, byte[]> _serializer;

        private readonly Func<byte[], object> _deserializer;

        public ValueSerializer()
        {
            var type = typeof(T);
            _serializer = GenerateSerializer(type);
            _deserializer = GenerateDeserializer(type);
        }

        public byte[] Serialize(object value)
        {
            return _serializer(value);
        }

        public object Deserialize(byte[] rawData)
        {
            return _deserializer(rawData);
        }

        private Func<object, byte[]> GenerateSerializer(Type type)
        {
            var typeCode = Type.GetTypeCode(type);

            switch (typeCode)
            {
                case TypeCode.Boolean:
                    return value => ValueSerialization.Serialize(ColumnType.Boolean, value);

                case TypeCode.Decimal:
                    return value => ValueSerialization.Serialize(ColumnType.Decimal, value);
                case TypeCode.Double:
                    return value => ValueSerialization.Serialize(ColumnType.Double, value);
                case TypeCode.Single:
                    return value => ValueSerialization.Serialize(ColumnType.Float, value);

                case TypeCode.Char:
                    return value => ValueSerialization.Serialize(ColumnType.Int, (int)(char)value);
                case TypeCode.Byte:
                    return value => ValueSerialization.Serialize(ColumnType.Int, (int)(byte)value);
                case TypeCode.SByte:
                    return value => ValueSerialization.Serialize(ColumnType.Int, (int)(sbyte)value);
                case TypeCode.UInt16:
                    return value => ValueSerialization.Serialize(ColumnType.Int, (int)(ushort)value);
                case TypeCode.Int16:
                    return value => ValueSerialization.Serialize(ColumnType.Int, (int)(short)value);
                case TypeCode.Int32:
                    return value => ValueSerialization.Serialize(ColumnType.Int, value);
                case TypeCode.UInt32:
                    return value => ValueSerialization.Serialize(ColumnType.Bigint, (long)(uint)value);
                case TypeCode.Int64:
                    return value => ValueSerialization.Serialize(ColumnType.Bigint, value);

                case TypeCode.String:
                    return value => ValueSerialization.Serialize(ColumnType.Varchar, value);

                case TypeCode.DateTime:
                    return value => ValueSerialization.Serialize(ColumnType.Timestamp, value);

                case TypeCode.Empty:
                    return null;

                default:
                    return GenerateObjectSerializer(type);
            }
        }

        private Func<object, byte[]> GenerateObjectSerializer(Type type)
        {
            var customSerializer = type.GetCustomAttributes(typeof(CassandraTypeSerializerAttribute), false).FirstOrDefault() as CassandraTypeSerializerAttribute;
            if (customSerializer != null)
            {
                var serializer = customSerializer.Serializer(type);
                // ReSharper disable once CanBeReplacedWithTryCastAndCheckForNull
                if( serializer is ICassandraGenericTypeSerializer )
                    return value => ((ICassandraGenericTypeSerializer)serializer).Serialize(value, GenerateObjectSerializer);

                return value => ((ICassandraTypeSerializer)serializer).Serialize(value);
            }

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return GenerateSerializer(type.GetGenericArguments()[0]);
            }

            if (typeof(Guid).IsAssignableFrom(type))
            {
                return value => ValueSerialization.Serialize(ColumnType.Uuid, value);
            }

            if (typeof(IPAddress).IsAssignableFrom(type))
            {
                return value => ValueSerialization.Serialize(ColumnType.Inet, value);
            }

            if (typeof(Enum).IsAssignableFrom(type))
            {
                var enumType = type.GetEnumUnderlyingType();
                return GenerateSerializer(enumType);
            }

            if (typeof(byte[]).IsAssignableFrom(type))
            {
                return value => ValueSerialization.Serialize(ColumnType.Blob, value);
            }

            if (typeof(DateTimeOffset).IsAssignableFrom(type))
            {
                return value => ValueSerialization.Serialize(ColumnType.Timestamp, ((DateTimeOffset) value).UtcDateTime);
            }


            if (typeof(IDictionary).IsAssignableFrom(type))
            {
                var dictionaryTypeDef = type.GetInterfaces().FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IDictionary<,>));
                if (dictionaryTypeDef != null)
                {
                    var dictionaryArgs = dictionaryTypeDef.GetGenericArguments();

                    var keySerializer = GenerateSerializer(dictionaryArgs[0]);
                    var valueSerializer = GenerateSerializer(dictionaryArgs[1]);

                    return value => ValueSerialization.SerializeMap((IDictionary)value, keySerializer, valueSerializer);
                }
            }

            if (typeof(IList).IsAssignableFrom(type))
            {
                var listTypeDef = type.GetInterfaces().FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IList<>));
                if (listTypeDef != null)
                {
                    var listItemType = listTypeDef.GetGenericArguments()[0];

                    var valueSerializer = GenerateSerializer(listItemType);

                    return value => ValueSerialization.SerializeList((IList)value, valueSerializer);
                }
            }

            var hashSetTypeDef = type.GetInterfaces().FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(ISet<>));
            if (hashSetTypeDef != null)
            {
                var setItemType = hashSetTypeDef.GetGenericArguments()[0];
                var valueSerializer = GenerateSerializer(setItemType);

                return value =>
                {
                    Type setAccessorType = typeof(HashSetAccessor<>).MakeGenericType(setItemType);
                    var hashSet = (IHashSetAccessor)Activator.CreateInstance(setAccessorType, value);
                    return ValueSerialization.SerializeSet(hashSet, valueSerializer);
                };
            }

            throw new NotSupportedException(string.Format("Type {0} neither belongs to Cassandra native types nor has custom serializer defined. Use CassandraTypeSerializerAttribute to define custom type serializer to store data inside Blob column.", type.FullName));
        }

        private Func<byte[], object> GenerateDeserializer(Type type)
        {
            var typeCode = Type.GetTypeCode(type);

            switch (typeCode)
            {
                case TypeCode.Boolean:
                    return value => ValueSerialization.Deserialize(ColumnType.Boolean, value);

                case TypeCode.Decimal:
                    return value => ValueSerialization.Deserialize(ColumnType.Decimal, value);
                case TypeCode.Double:
                    return value => ValueSerialization.Deserialize(ColumnType.Double, value);
                case TypeCode.Single:
                    return value => ValueSerialization.Deserialize(ColumnType.Float, value);

                case TypeCode.Char:
                    return value => (char)(int)ValueSerialization.Deserialize(ColumnType.Int, value);
                case TypeCode.Byte:
                    return value => (byte)(int)ValueSerialization.Deserialize(ColumnType.Int, value);
                case TypeCode.SByte:
                    return value => (sbyte)(int)ValueSerialization.Deserialize(ColumnType.Int, value);
                case TypeCode.UInt16:
                    return value => (ushort)(int)ValueSerialization.Deserialize(ColumnType.Int, value);
                case TypeCode.Int16:
                    return value => (short)(int)ValueSerialization.Deserialize(ColumnType.Int, value);
                case TypeCode.Int32:
                    return value => ValueSerialization.Deserialize(ColumnType.Int, value);
                case TypeCode.UInt32:
                    return value => (uint)(long)ValueSerialization.Deserialize(ColumnType.Bigint, value);
                case TypeCode.Int64:
                    return value => ValueSerialization.Deserialize(ColumnType.Bigint, value);

                case TypeCode.String:
                    return value => ValueSerialization.Deserialize(ColumnType.Varchar, value);

                case TypeCode.DateTime:
                    return value => ValueSerialization.Deserialize(ColumnType.Timestamp, value);

                default:
                    return GenerateObjectDeserializer(type);
            }
        }

        private Func<byte[], object> GenerateObjectDeserializer(Type type)
        {
            var customSerializer = type.GetCustomAttributes(typeof(CassandraTypeSerializerAttribute), false).FirstOrDefault() as CassandraTypeSerializerAttribute;
            if (customSerializer != null)
            {
                var serializer = customSerializer.Serializer(type);
                // ReSharper disable once CanBeReplacedWithTryCastAndCheckForNull
                if (serializer is ICassandraGenericTypeSerializer)
                    return value => ((ICassandraGenericTypeSerializer)serializer).Deserialize(value, GenerateObjectDeserializer);

                return value => ((ICassandraTypeSerializer)serializer).Deserialize(value);
            }

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return GenerateDeserializer(type.GetGenericArguments()[0]);
            }

            if (typeof(Guid).IsAssignableFrom(type))
            {
                return value => ValueSerialization.Deserialize(ColumnType.Uuid, value);
            }

            if (typeof(IPAddress).IsAssignableFrom(type))
            {
                return value => ValueSerialization.Deserialize(ColumnType.Inet, value);
            }

            if (typeof(Enum).IsAssignableFrom(type))
            {
                var enumType = type.GetEnumUnderlyingType();
                return GenerateDeserializer(enumType);
            }

            if (typeof(byte[]).IsAssignableFrom(type))
            {
                return value => ValueSerialization.Deserialize(ColumnType.Blob, value);
            }
            if (typeof(DateTimeOffset).IsAssignableFrom(type))
            {
                // DateTime deserializer assumes local, but the value is truely utx
                return value => (DateTimeOffset) DateTime.SpecifyKind((DateTime) ValueSerialization.Deserialize(ColumnType.Timestamp, value), DateTimeKind.Local);
            }

            if (typeof(IDictionary).IsAssignableFrom(type))
            {
                var dictionaryTypeDef = type.GetInterfaces().FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IDictionary<,>));
                if (dictionaryTypeDef != null)
                {
                    var dictionaryArgs = dictionaryTypeDef.GetGenericArguments();

                    var keyDeserializer = GenerateDeserializer(dictionaryArgs[0]);
                    var valueDeserializer = GenerateDeserializer(dictionaryArgs[1]);

                    return rawData =>
                    {
                        var res = (IDictionary)Activator.CreateInstance(type);
                        return ValueSerialization.DeserializeMap(rawData, res, keyDeserializer, valueDeserializer);
                    };
                }
            }

            if (typeof(IList).IsAssignableFrom(type))
            {
                var listTypeDef = type.GetInterfaces().FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IList<>));
                if (listTypeDef != null)
                {
                    var listItemType = listTypeDef.GetGenericArguments()[0];
                    var valueSerializer = GenerateDeserializer(listItemType);

                    return rawData =>
                    {
                        var res = (IList)Activator.CreateInstance(type);
                        return ValueSerialization.DeserializeList(rawData, res, valueSerializer);
                    };
                }
            }

            var hashSetTypeDef = type.GetInterfaces().FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(ISet<>));
            if (hashSetTypeDef != null)
            {
                var setItemType = hashSetTypeDef.GetGenericArguments()[0];
                var valueDeserializer = GenerateDeserializer(setItemType);

                return rawData =>
                {
                    var hashSet = Activator.CreateInstance(type);
                    Type accessorType = typeof(HashSetAccessor<>).MakeGenericType(setItemType);
                    var setAccessor = (IHashSetAccessor)Activator.CreateInstance(accessorType, hashSet);

                    ValueSerialization.DeserializeSet(rawData, setAccessor, valueDeserializer);
                    return hashSet;
                };
            }

            throw new NotSupportedException(string.Format("Type {0} neither belongs to Cassandra native types nor has custom serializer defined. Use CassandraTypeSerializerAttribute to define custom type serializer to store data inside Blob column.", type.FullName));
        }
    }
}
