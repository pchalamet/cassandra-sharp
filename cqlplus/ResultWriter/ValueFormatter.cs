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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace cqlplus.ResultWriter
{
    public static class ValueFormatter
    {
        private static bool IsGenericCollection(Type type)
        {
            var collType = type.GetInterfaces()
                               .Where(face => face.IsGenericType &&
                                              face.GetGenericTypeDefinition() == typeof(ICollection<>))
                               .Select(face => face.GetGenericArguments()[0])
                               .FirstOrDefault();
            return null != collType;
        }

        public static string Format(object value)
        {
            if (null == value) return string.Empty;

            // dictionary
            var map = value as IDictionary;
            if (null != map)
            {
                var sb = new StringBuilder();
                var sep = "";
                foreach (DictionaryEntry dictionaryEntry in map)
                {
                    sb.Append(sep).Append(dictionaryEntry.Key).Append("=").Append(dictionaryEntry.Value);
                    sep = " ";
                }

                return sb.ToString();
            }

            // byte array
            var byteArray = value as byte[];
            if (null != byteArray)
            {
                var sb = new StringBuilder();
                foreach (var b in byteArray) sb.AppendFormat("{0:X2}", b);

                return sb.ToString();
            }

            // ICollection<T>
            if (IsGenericCollection(value.GetType()))
            {
                var coll = (IEnumerable)value;
                var sb = new StringBuilder();
                var sep = "";
                foreach (var elem in coll)
                {
                    sb.Append(sep).Append(elem);
                    sep = " ";
                }

                return sb.ToString();
            }

            // can't do much better
            return value.ToString();
        }
    }
}