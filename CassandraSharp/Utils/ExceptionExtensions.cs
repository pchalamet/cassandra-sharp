// cassandra-sharp - a .NET client for Apache Cassandra
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

namespace CassandraSharp.Utils
{
    using System;
    using System.Reflection;

    internal static class ExceptionExtensions
    {
        private static readonly Action<Exception> _preserveInternalException;

        static ExceptionExtensions()
        {
            MethodInfo preserveStackTrace = typeof(Exception).GetMethod("InternalPreserveStackTrace", BindingFlags.Instance | BindingFlags.NonPublic);
            _preserveInternalException = (Action<Exception>) Delegate.CreateDelegate(typeof(Action<Exception>), preserveStackTrace);
        }

        public static void RethrowPreserveStackTrace(this Exception @this)
        {
            _preserveInternalException(@this);
            throw @this;
        }
    }
}