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

namespace cqlplus
{
    using System;
    using System.Net;
    using System.Threading;
    using CassandraSharp;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;

    public class ConsoleInstrumentation : IInstrumentation
    {
        public void ClientQuery(InstrumentationToken token)
        {
            if (CommandContext.DebugLog)
            {
                string buffer = string.Format("INSTR {0} [{1}] - id:{2}",
                                              DateTime.Now, Thread.CurrentThread.ManagedThreadId,
                                              token.Id);
                Console.WriteLine(buffer);
            }
        }

        public void ClientConnectionInfo(InstrumentationToken token, IPAddress coordinator, byte streamId)
        {
            if (CommandContext.DebugLog)
            {
                string buffer = string.Format("INSTR {0} [{1}] - id:{2} type:{3} coordinator:{4} streamId:{5} cql:{6}",
                                              DateTime.Now, Thread.CurrentThread.ManagedThreadId,
                                              token.Id, token.Type, coordinator, streamId, token.Cql);
                Console.WriteLine(buffer);
            }
        }

        public void ClientTrace(InstrumentationToken token, EventType eventType)
        {
            if (CommandContext.DebugLog)
            {
                string buffer = string.Format("INSTR {0} [{1}] - id:{2} type:{3}",
                                              DateTime.Now, Thread.CurrentThread.ManagedThreadId,
                                              token.Id, eventType);
                Console.WriteLine(buffer);
            }
        }

        public void ServerTrace(InstrumentationToken token, TracingSession session)
        {
        }
    }
}