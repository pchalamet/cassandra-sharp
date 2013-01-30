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

    public class ConsoleInstrumentation : IInstrumentation
    {
        private readonly object _lock = new object();

        public void ClientTrace(IPAddress coordinator, byte streamId, CheckpointType type)
        {
            lock (_lock)
            {
                if (CommandContext.DebugLog)
                {
                    Console.Write("INSTR {0} [{1}] - ", DateTime.Now, Thread.CurrentThread.ManagedThreadId);
                    Console.WriteLine("coordinator:{0} streamId:{1} type:{2}", coordinator, streamId, type);
                }
            }
        }

        public void ServerTrace(IPAddress coordinator, byte streamId, TracingSession session)
        {
        }
    }
}