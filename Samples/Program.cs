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

namespace Samples
{
    using System;
    using Samples.Async;
    using Samples.Future;
    using Samples.Linq;
    using Samples.POCO;
    using Samples.PreparedStatement;
    using Samples.Stress;
    using Samples.TimeOut;

    internal class Program
    {
        private static void Main(string[] args)
        {
            Stress.ResilienceTest test = new ResilienceTest();
            test.RecoveryTest();

            //new TimeOutSample().Run();
            //new BatchSample().Run();
            //new LinqSample().Run();
            //new FutureSample().Run();
            //new PocoSample().Run();
            //new AsyncSample().Run();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }
    }
}