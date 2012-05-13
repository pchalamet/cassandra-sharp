// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace CassandraSharpUnitTests.MadeSimple
{
    using CassandraSharp.MadeSimple;
    using NUnit.Framework;

    [TestFixture]
    public class IntNameOrValueTest
    {
        [Test]
        public void TestIntName()
        {
            IntNameOrValue intNameOrValue = new IntNameOrValue(0x04030201);
            Assert.IsTrue(intNameOrValue.Value == 0x04030201);

            byte[] buffer = intNameOrValue.ToByteArray();
            Assert.IsTrue(buffer[0] == 0x04);
            Assert.IsTrue(buffer[1] == 0x03);
            Assert.IsTrue(buffer[2] == 0x02);
            Assert.IsTrue(buffer[3] == 0x01);

            IntNameOrValue intName2 = new IntNameOrValue(buffer);
            Assert.IsTrue(intName2.Value == 0x04030201);

            Assert.IsTrue(intName2.RawValue is int);
        }
    }
}