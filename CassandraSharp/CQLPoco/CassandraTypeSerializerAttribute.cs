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
using System.Linq;

namespace CassandraSharp.CQLPoco
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
    public class CassandraTypeSerializerAttribute : Attribute
    {
        public CassandraTypeSerializerAttribute(Type serializer)
        {
            // Contract, targetted implementation type must implement our interface
            if (!typeof(ICassandraTypeSerializer).IsAssignableFrom(serializer))
                throw new ArgumentException(string.Format("{0} does not implement ICassandraTypeSerializer interface", serializer));

            // Store the type for consumer-code introspection
            TypeSerializer = serializer;

            // Get the public constructors
            var ctors = serializer.GetConstructors();

            // Determine if there is a constructor which passed the serialized object's type
            var typeCtor = ctors.Where(ctor =>
                                       {
                                           var prms = ctor.GetParameters();
                                           return prms.Length == 1 && prms.First().ParameterType == typeof(Type);
                                       }).FirstOrDefault();

            // Determine if there is a constructor which passed the serialized object's type as well as access to the default serializer and deserializer (recursable)
            var recurseCtor = ctors.Where(ctor =>
                                          {
                                              var prms = ctor.GetParameters().ToArray();
                                              return prms.Length == 3 && prms[0].ParameterType == typeof(Type) &&
                                                     prms[1].ParameterType == typeof(Func<Type, Func<object, byte[]>>) &&
                                                     prms[2].ParameterType == typeof(Func<Type, Func<byte[], object>>);
                                          }).FirstOrDefault();

            // Populate the Serializer field with the appropriate delegate
            if (recurseCtor != null)
                Serializer = (type, defaultSerializer, defaultDeserializer) =>
                                 TypeFactory.CreateSerializer(serializer, type, defaultSerializer, defaultDeserializer);
            else if (typeCtor != null)
                Serializer = (type, defaultSerializer, defaultDeserializer) => TypeFactory.CreateSerializer(serializer, type);
            else
                Serializer = (type, defaultSerializer, defaultDeserializer) => TypeFactory.CreateSerializer(serializer);
        }

        public Type TypeSerializer { get; }

        public Func<Type, Func<Type, Func<object, byte[]>>, Func<Type, Func<byte[], object>>, ICassandraTypeSerializer> Serializer { get; }
    }
}