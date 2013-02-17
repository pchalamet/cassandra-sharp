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

namespace CassandraSharp
{
    public enum MessageOpcodes
    {
        Error = 0x00,

        Startup = 0x01,

        Ready = 0x02,

        Authenticate = 0x03,

        Credentials = 0x04,

        Options = 0x05,

        Supported = 0x06,

        Query = 0x07,

        Result = 0x08,

        Prepare = 0x09,

        Execute = 0x0A,

        Register = 0x0B,

        Event = 0x0C,
    }
}