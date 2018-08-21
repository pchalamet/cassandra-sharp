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
    public enum ErrorCodes
    {
        Server = 0x0000,

        Protocol = 0x000A,

        Unavailable = 0x1000,

        Overloaded = 0x1001,

        IsBootstrapping = 0x1002,

        Truncate = 0x1003,

        WriteTimeout = 0x1100,

        ReadTimeout = 0x1200,

        Syntax = 0x2000,

        Unauthorized = 0x2100,

        Invalid = 0x2200,

        Config = 0x2300,

        AlreadyExists = 0x2400,

        Unprepared = 0x2500,

        Unknown = 0xFFFF
    }
}