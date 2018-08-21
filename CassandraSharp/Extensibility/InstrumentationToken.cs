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

namespace CassandraSharp.Extensibility
{
    public class InstrumentationToken : IEquatable<InstrumentationToken>
    {
        private InstrumentationToken(RequestType type, ExecutionFlags executionFlags, string cql)
        {
            Id = Guid.NewGuid();
            Type = type;
            ExecutionFlags = executionFlags;
            Cql = cql ?? string.Empty;
        }

        public string Cql { get; }

        public Guid Id { get; }

        public RequestType Type { get; }

        public ExecutionFlags ExecutionFlags { get; }

        public bool Equals(InstrumentationToken other)
        {
            if (null == other) return false;

            var bRes = Id == other.Id
                       && Type == other.Type
                       && ExecutionFlags == other.ExecutionFlags
                       && Cql == other.Cql;
            return bRes;
        }

        public override int GetHashCode()
        {
            const int prime = 31;
            var hash = 0;
            hash = prime * (hash + Id.GetHashCode());
            hash += prime * (hash + Type.GetHashCode());
            hash += prime * (hash + ExecutionFlags.GetHashCode());
            hash += prime * (hash + Cql.GetHashCode());
            return hash;
        }

        public override bool Equals(object obj)
        {
            var other = obj as InstrumentationToken;
            if (null != other) return Equals(other);

            return false;
        }

        internal static InstrumentationToken Create(RequestType requestType, ExecutionFlags executionFlags, string cql = null)
        {
            return new InstrumentationToken(requestType, executionFlags, cql);
        }
    }
}