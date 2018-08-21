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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using CassandraSharp;
using NUnit.Framework;

namespace CassandraSharpUnitTests.Enlightenment
{
    [TestFixture]
    public class FutureTest
    {
        private IEnumerable<int> FailureStream()
        {
            yield return 1;
            throw new ApplicationException("FailureStream");
        }

        [Test]
        public void TestAsFuture()
        {
            var data = Enumerable.Range(0, 10);
            var obsData = data.ToObservable();

            var futData = obsData.AsFuture();
            Assert.AreEqual(futData.Result.Count, 10);
        }

        [Test]
        public void TestAsFutureException()
        {
            var obsData = FailureStream().ToObservable();

            var futData = obsData.AsFuture();
            try
            {
                var len = futData.Result.Count;
            }
            catch (Exception ex)
            {
                // this is TPL
                Assert.IsTrue(ex is AggregateException);

                // this is our exception
                Assert.IsTrue(ex.InnerException is ApplicationException);
            }
        }
    }
}