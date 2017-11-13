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

namespace CassandraSharp
{
    public enum ConsistencyLevel
    {
// ReSharper disable InconsistentNaming
        ANY = 0x0000,

        ONE = 0x0001,

        TWO = 0x0002,

        THREE = 0x0003,

        QUORUM = 0x0004,

        ALL = 0x0005,

        LOCAL_QUORUM = 0x0006,

        EACH_QUORUM = 0x0007,

        SERIAL = 0x0008,

        LOCAL_SERIAL = 0x0009,

        LOCAL_ONE = 0x000A
        // ReSharper restore InconsistentNaming
    }
}