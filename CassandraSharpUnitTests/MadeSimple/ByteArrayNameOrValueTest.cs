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

namespace CassandraSharpUnitTests.MadeSimple
{
    using CassandraSharp.MadeSimple;
    using NUnit.Framework;

    [TestFixture]
    public class ByteArrayNameOrValueTest
    {
        [Test]
        public void TestByteArrayName()
        {
            ByteArrayNameOrValue byteArrayNameOrValue = new ByteArrayNameOrValue(new byte[] {1, 2, 3, 4});
            Assert.IsTrue(byteArrayNameOrValue.Value.Length == 4);
            Assert.IsTrue(byteArrayNameOrValue.Value[0] == 1);
            Assert.IsTrue(byteArrayNameOrValue.Value[1] == 2);
            Assert.IsTrue(byteArrayNameOrValue.Value[2] == 3);
            Assert.IsTrue(byteArrayNameOrValue.Value[3] == 4);

            byte[] buffer = byteArrayNameOrValue.ToByteArray();
            Assert.IsTrue(buffer.Length == 4);
            Assert.IsTrue(buffer[0] == 1);
            Assert.IsTrue(buffer[1] == 2);
            Assert.IsTrue(buffer[2] == 3);
            Assert.IsTrue(buffer[3] == 4);

            ByteArrayNameOrValue byteArrayName2 = new ByteArrayNameOrValue(buffer);
            Assert.IsTrue(byteArrayName2.Value.Length == 4);
            Assert.IsTrue(byteArrayName2.Value[0] == 1);
            Assert.IsTrue(byteArrayName2.Value[1] == 2);
            Assert.IsTrue(byteArrayName2.Value[2] == 3);
            Assert.IsTrue(byteArrayName2.Value[3] == 4);

            Assert.IsTrue(byteArrayName2.RawValue is byte[]);
        }
    }
}