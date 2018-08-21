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

namespace CassandraSharp.CQLPoco
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public sealed class CqlColumnAttribute : Attribute
    {
        public CqlColumnAttribute(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Column name cannot be null or empty.");

            Name = name;
        }

        public string Name { get; }
    }
}