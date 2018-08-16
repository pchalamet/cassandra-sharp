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
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Linq;
using CassandraSharp;
using CassandraSharp.CQLPoco;

namespace CassandraSharp
{
    internal class TypeFactory
    {
        private static readonly object _lock = new object();
        private static readonly Dictionary<Type, ICassandraTypeSerializer> _serializers = new Dictionary<Type, ICassandraTypeSerializer>();

        public static TI Create<TI>(Type type, object[] prms)
        {
            // mini-dependency injection
            ConstructorInfo ci = type.GetConstructors().Single();

            // Note: parameters must all be of different types
            ParameterInfo[] pis = ci.GetParameters();

            // Create an array to ensure proper ordering of parameters
            object[] ciPrms = new object[pis.Length];

            // Populate the array.  Missing parameters cause exceptions
            for (int idx = 0; idx < ciPrms.Length; ++idx)
            {
                // Get parameter type
                Type piType = pis[idx].ParameterType;

                // IsInstanceOfType (and other methods) do not honor generic parameters
                // For Serializer, we must differenciate the Func`2, so we use the order of parameters
                // .First will throw an exception if prms is not proper, which was the previous behavior
                var parameterIndex = prms.Select((o, i) => piType.IsInstanceOfType(o) ? i : -1).First(i => i >= 0);
                if (parameterIndex >= 0)
                {
                    var temp = parameterIndex >= 0 ? prms[parameterIndex] : null;
                    prms[parameterIndex] = null;
                    ciPrms[idx] = temp;
                }
            }

            // Create instance from type variable, cast to interface specified by the type parameter
            return (TI)Activator.CreateInstance(type, ciPrms);
        }


        // Create a serializer with the default constructor
        public static ICassandraTypeSerializer CreateSerializer(Type serializer)
        {
            var parameters = new object[] { };
            return CreateAndCacheSerializer(serializer, parameters);
        }

        // Create a serializer with the target type passed in the constructor
        public static ICassandraTypeSerializer CreateSerializer(Type serializer, Type serializedType)
        {
            var parameters = new object[] { serializedType };
            return CreateAndCacheSerializer(serializer, parameters);
        }

        // Create a serializer with the target type passed in the constructor, as well as access to the default serializer and deserializer
        public static ICassandraTypeSerializer CreateSerializer(Type serializer, Type serializedType, Func<Type, Func<object, byte[]>> defaultSerializer, Func<Type, Func<byte[], object>> defaultDeserializer)
        {
            var parameters = new object[] { serializedType, defaultSerializer, defaultDeserializer };
            return CreateAndCacheSerializer(serializer, parameters);
        }

        private static ICassandraTypeSerializer CreateAndCacheSerializer(Type serializer, object[] parameters)
        {
            // Double-check pattern works as lock/read optimization works in CLR (but not JVM!)
            // Lazy<T> in .NET 4.0 explicitly used this pattern.  
            // Lazy<T> in .NET 4.5 does a similar check against the lock object itself.
            // Furthermore, the ConcurrentDictionary does not guarantee that the addValueFactory will only be called once.  
            // While only small improvement, this is the most optimized approach.
            // ReSharper disable once InconsistentlySynchronizedField
            if (!_serializers.ContainsKey(serializer))
            {
                lock (_lock)
                {
                    if (!_serializers.ContainsKey(serializer))
                    {
                        var instance = Create<ICassandraTypeSerializer>(serializer, parameters);
                        _serializers.Add(serializer, instance);
                        return instance;
                    }
                }
            }

            // ReSharper disable once InconsistentlySynchronizedField
            return _serializers[serializer];
        }
    }
}
