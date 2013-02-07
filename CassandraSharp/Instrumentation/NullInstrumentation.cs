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
    using System.Net;
    using CassandraSharp.Extensibility;

    public class NullInstrumentation : IInstrumentation
    {
        public void ClientQuery(InstrumentationToken token)
        {
        }

        public void ClientConnectionInfo(InstrumentationToken token, IPAddress coordinator, byte streamId)
        {
        }

        public void ClientTrace(InstrumentationToken token, EventType eventType)
        {
        }

        public void ServerTrace(InstrumentationToken token, TracingSession session)
        {
        }
    }
}