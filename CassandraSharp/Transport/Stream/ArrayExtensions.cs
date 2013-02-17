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

namespace CassandraSharp.Transport.Stream
{
    using System;

    internal static class ArrayExtensions
    {
        public static void ReverseIfLittleEndian(this byte[] buffer)
        {
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(buffer);
            }
        }

        public static void ReverseIfLittleEndian(this byte[] buffer, int index, int length)
        {
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(buffer, index, length);
            }
        }
    }
}