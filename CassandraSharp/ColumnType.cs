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

namespace CassandraSharp
{
    public enum ColumnType
    {
        Custom = 0x0000,

        Ascii = 0x0001,

        Bigint = 0x0002,

        Blob = 0x0003,

        Boolean = 0x0004,

        Counter = 0x0005,

        Decimal = 0x0006,

        Double = 0x0007,

        Float = 0x0008,

        Int = 0x0009,

        Timestamp = 0x000B,

        Uuid = 0x000C,

        Varchar = 0x000D,

        Varint = 0x000E,

        Timeuuid = 0x000F,

        Inet = 0x0010,

        Date = 0x0011,

        Time = 0x0012,

        SmallInt = 0x0013,

        TinyInt = 0x14,

        List = 0x0020,

        Map = 0x0021,

        Set = 0x0022
    }
}