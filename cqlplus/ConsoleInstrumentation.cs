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
    using System.Text;
    using System.Threading;
    using CassandraSharp;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;

    public class ConsoleInstrumentation : IInstrumentation
    {
        public void ClientQuery(InstrumentationToken token)
        {
            if (0 != (token.ExecutionFlags & ExecutionFlags.ClientTracing))
            {
                string buffer = string.Format("INSTR {0} [{1}] - id:{2}",
                                              DateTime.Now, Thread.CurrentThread.ManagedThreadId,
                                              token.Id);
                Console.WriteLine(buffer);
            }
        }

        public void ClientConnectionInfo(InstrumentationToken token, IPAddress coordinator, byte streamId)
        {
            if (0 != (token.ExecutionFlags & ExecutionFlags.ClientTracing))
            {
                string buffer = string.Format("INSTR {0} [{1}] - id:{2} type:{3} coordinator:{4} streamId:{5} cql:{6}",
                                              DateTime.Now, Thread.CurrentThread.ManagedThreadId,
                                              token.Id, token.Type, coordinator, streamId, token.Cql);
                Console.WriteLine(buffer);
            }
        }

        public void ClientTrace(InstrumentationToken token, EventType eventType)
        {
            if (0 != (token.ExecutionFlags & ExecutionFlags.ClientTracing))
            {
                string buffer = string.Format("INSTR {0} [{1}] - id:{2} type:{3}",
                                              DateTime.Now, Thread.CurrentThread.ManagedThreadId,
                                              token.Id, eventType);
                Console.WriteLine(buffer);
            }
        }

        public void ServerTrace(InstrumentationToken token, TracingSession tracingSession)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("INSTR {0} [{1}] - ",
                            DateTime.Now, Thread.CurrentThread.ManagedThreadId);
            int len = sb.Length;
            string offset = new string(' ', len);

            sb.AppendFormat("sessionId:{0} startedAt:{1} coordinator:{2} duration:{3} request:{4}",
                            tracingSession.SessionId, tracingSession.StartedAt, tracingSession.Coordinator, tracingSession.Duration,
                            tracingSession.Parameters["query"]);
            foreach (TracingEvent tracingEvent in tracingSession.TracingEvents)
            {
                sb.AppendLine();
                sb.Append(offset);
                sb.AppendFormat("sourceElapsed:{0} activity:{1} thread:{2}", tracingEvent.SourceElapsed, tracingEvent.Activity, tracingEvent.Thread);
            }

            Console.WriteLine(sb);
        }
    }
}