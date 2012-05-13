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
    public class LongNameOrValueTest
    {
        [Test]
        public void TestLongName()
        {
            LongNameOrValue longNameOrValue = new LongNameOrValue(0x0807060504030201);
            Assert.IsTrue(longNameOrValue.Value == 0x0807060504030201);

            byte[] buffer = longNameOrValue.ToByteArray();
            Assert.IsTrue(buffer[0] == 0x08);
            Assert.IsTrue(buffer[1] == 0x07);
            Assert.IsTrue(buffer[2] == 0x06);
            Assert.IsTrue(buffer[3] == 0x05);
            Assert.IsTrue(buffer[4] == 0x04);
            Assert.IsTrue(buffer[5] == 0x03);
            Assert.IsTrue(buffer[6] == 0x02);
            Assert.IsTrue(buffer[7] == 0x01);

            LongNameOrValue longName2 = new LongNameOrValue(buffer);
            Assert.IsTrue(longName2.Value == 0x0807060504030201);

            Assert.IsTrue(longName2.RawValue is long);
        }
    }
}