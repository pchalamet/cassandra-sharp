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

namespace cqlsh.ResultWriter
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;

    public static class ValueFormatter
    {
        private static bool IsGenericCollection(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ICollection<>);
        }

        public static string Format(object value)
        {
            if (null == value)
            {
                return string.Empty;
            }

            // dictionary
            IDictionary map = value as IDictionary;
            if (null != map)
            {
                StringBuilder sb = new StringBuilder();
                string sep = "";
                foreach (DictionaryEntry dictionaryEntry in map)
                {
                    sb.Append(sep).Append(dictionaryEntry.Key).Append("=").Append(dictionaryEntry.Value);
                    sep = " ";
                }

                return sb.ToString();
            }

            // byte array
            byte[] byteArray = value as byte[];
            if (null != byteArray)
            {
                StringBuilder sb = new StringBuilder();
                foreach (byte b in byteArray)
                {
                    sb.AppendFormat("{0:X2}", b);
                }

                return sb.ToString();
            }

            // ICollection<T>
            if (IsGenericCollection(value.GetType()))
            {
                IEnumerable coll = (IEnumerable) value;
                StringBuilder sb = new StringBuilder();
                string sep = "";
                foreach (object elem in coll)
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