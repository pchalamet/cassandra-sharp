// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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

namespace CassandraSharpUnitTests.Pool
{
    using System;
    using CassandraSharp;
    using CassandraSharp.Pool;
    using Moq;
    using NUnit.Framework;

    [TestFixture]
    public class VoidPoolTest
    {
        [Test]
        public void TestBehavior()
        {
            Mock<IDisposable> mock = new Mock<IDisposable>();
            Token token = new Token(new byte[0]);

            IPool<Token, IDisposable> pool = new VoidPool<Token, IDisposable>();

            // nothing should be aquired
            IDisposable disposable;
            Assert.IsFalse(pool.Acquire(token, out disposable));
            Assert.IsNull(disposable);

            // should be released
            pool.Release(token, mock.Object);
            mock.Verify(x => x.Dispose());

            // nothing should be acquired
            Assert.IsFalse(pool.Acquire(token, out disposable));
            Assert.IsNull(disposable);

            // dispose must dispose everything
            pool.Dispose();
        }
    }
}