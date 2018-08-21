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
using System.Collections.Generic;

namespace CassandraSharp.Utils
{
    internal static class ArrayExtensions
    {
        public static void ReverseIfLittleEndian(this byte[] buffer)
        {
            if (BitConverter.IsLittleEndian) Array.Reverse(buffer);
        }

        public static void ReverseIfLittleEndian(this byte[] buffer, int index, int length)
        {
            if (BitConverter.IsLittleEndian) Array.Reverse(buffer, index, length);
        }

        public static T[] BinaryAdd<T>(this T[] @this, T t, IComparer<T> comparer)
        {
            var idx = Array.BinarySearch(@this, t, comparer);
            if (idx < 0)
            {
                idx = ~idx;
                var oldLen = @this.Length;
                var newArray = new T[oldLen + 1];
                if (0 == idx)
                {
                    Array.Copy(@this, 0, newArray, 1, oldLen);
                    newArray[0] = t;
                }
                else if (oldLen == idx)
                {
                    Array.Copy(@this, 0, newArray, 0, oldLen);
                    newArray[oldLen] = t;
                }
                else
                {
                    Array.Copy(@this, 0, newArray, 0, idx);
                    Array.Copy(@this, idx, newArray, idx + 1, oldLen - idx);
                    newArray[idx] = t;
                }

                return newArray;
            }

            @this[idx] = t;
            return @this;
        }

        public static void BinaryRemove<T>(this T[] @this, T t, IComparer<T> comparer)
        {
            var idx = Array.BinarySearch(@this, t, comparer);
            if (idx >= 0)
            {
                var newLen = @this.Length - 1;
                var partitions = new T[newLen];
                Array.Copy(@this, 0, partitions, 0, idx);
                Array.Copy(@this, idx, partitions, idx, newLen - idx);
            }
        }
    }
}