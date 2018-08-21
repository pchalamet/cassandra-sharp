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

namespace CassandraSharpUnitTests.Utils
{
    using System.Collections.Generic;
    using NUnit.Framework;
    using CassandraSharp.Utils;

    [TestFixture]
    public class ArrayExtensionsTest
    {
        public class IntComparer : IComparer<int>
        {
            public int Compare(int x, int y)
            {
                if (x < y)
                {
                    return -1;
                }

                if (x > y)
                {
                    return 1;
                }

                return 0;
            }
        }

        [Test]
        public void AddElement()
        {
            IComparer<int> comparer = new IntComparer();

            // []
            var array = new int[0];

            // [10]
            array = array.BinaryAdd(10, comparer);
            Assert.IsTrue(array.Length == 1);
            Assert.IsTrue(array[0] == 10);

            // [0, 10]
            array = array.BinaryAdd(0, comparer);
            Assert.IsTrue(array.Length == 2);
            Assert.IsTrue(array[0] == 0);
            Assert.IsTrue(array[1] == 10);

            // [0, 8, 10]
            array = array.BinaryAdd(8, comparer);
            Assert.IsTrue(array.Length == 3);
            Assert.IsTrue(array[0] == 0);
            Assert.IsTrue(array[1] == 8);
            Assert.IsTrue(array[2] == 10);

            // [0, 8, 10]
            array = array.BinaryAdd(8, comparer);
            Assert.IsTrue(array.Length == 3);
            Assert.IsTrue(array[0] == 0);
            Assert.IsTrue(array[1] == 8);
            Assert.IsTrue(array[2] == 10);
        }
    }
}