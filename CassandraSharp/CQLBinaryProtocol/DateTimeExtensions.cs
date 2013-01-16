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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;

    internal static class DateTimeExtensions
    {
        public static readonly DateTime _epoch = new DateTime(1970, 1, 1);

        public static long ToTimestamp(this DateTime dt)
        {
            TimeSpan t = (dt - _epoch);
            long timestamp = (long) t.TotalSeconds;
            return timestamp;
        }

        public static DateTime FromTimestamp(this long ts)
        {
            TimeSpan timeSpan = TimeSpan.FromSeconds(ts);
            DateTime date = _epoch + timeSpan;
            return date;
        }
    }
}