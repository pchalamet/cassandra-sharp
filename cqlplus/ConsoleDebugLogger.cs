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

namespace cqlplus
{
    using System;
    using System.Text;
    using System.Threading;
    using CassandraSharp.Extensibility;

    internal class ConsoleDebugLogger : ILogger
    {
        public void Debug(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Info(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Warn(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Error(string format, params object[] prms)
        {
            Log(format, prms);
        }

        public void Fatal(string format, params object[] prms)
        {
            Log(format, prms);
        }

        private static void Log(string format, object[] prms)
        {
            if (CommandContext.DebugLog)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("LOG   {0} [{1}] - ", DateTime.Now, Thread.CurrentThread.ManagedThreadId);
                sb.AppendFormat(format, prms);
                Console.WriteLine(sb);
            }
        }

        public void Dispose()
        {
        }
    }
}