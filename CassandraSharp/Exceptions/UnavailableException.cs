// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
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

namespace CassandraSharp.Exceptions
{
    using System;

    [Serializable]
    public class UnavailableException : CassandraException
    {
        public UnavailableException(string message, ConsistencyLevel consistencyLevel, int required, int alive)
                : base(ErrorCodes.Unavailable, message)
        {
            ConsistencyLevel = consistencyLevel;
            Required = required;
            Alive = alive;
        }

        public ConsistencyLevel ConsistencyLevel { get; private set; }

        public int Required { get; private set; }

        public int Alive { get; private set; }
    }
}