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

namespace CassandraSharp.Instrumentation
{
    using System;

    public class InstrumentationToken
    {
        private InstrumentationToken(RequestType type, ExecutionFlags executionFlags, string cql)
        {
            Id = Guid.NewGuid();
            Type = type;
            ExecutionFlags = executionFlags;
            Cql = cql;
        }

        public string Cql { get; private set; }

        public Guid Id { get; private set; }

        public RequestType Type { get; private set; }

        public ExecutionFlags ExecutionFlags { get; private set; }

        internal static InstrumentationToken Create(RequestType requestType, ExecutionFlags executionFlags, string cql = null)
        {
            return new InstrumentationToken(requestType, executionFlags, cql);
        }
    }
}