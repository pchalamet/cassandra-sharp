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

namespace CassandraSharp.Utils
{
    using System;
    using System.Diagnostics;

    internal static class CheckParameterExtensions
    {
        [Conditional("DEBUG")]
        public static void CheckArgumentNotNull(this object @this, string name)
        {
            if (null == @this)
            {
                throw new ArgumentNullException(name);
            }
        }

        [Conditional("DEBUG")]
        public static void CheckArrayHasAtLeastOneElement<T>(this T[] @this, string name)
        {
            @this.CheckArgumentNotNull(name);
            if (0 == @this.Length)
            {
                throw new ArgumentException("Array must have at least one element", name);
            }
        }

        [Conditional("DEBUG")]
        public static void CheckArrayIsSameLengthAs<T1, T2>(this T1[] @this, T2[] otherPrm, string thisName, string otherName)
        {
            @this.CheckArgumentNotNull(thisName);
            otherPrm.CheckArgumentNotNull(otherName);
            if (@this.Length != otherPrm.Length)
            {
                throw new ArgumentException("Arrays must have the same length", thisName + "/" + otherName);
            }
        }
    }
}