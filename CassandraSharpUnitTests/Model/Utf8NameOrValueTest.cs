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
namespace CassandraSharpUnitTests.Model
{
    using CassandraSharp.NameOrValues;
    using NUnit.Framework;

    [TestFixture]
    public class Utf8NameOrValueTest
    {
        [Test]
        public void TestUtf8Name()
        {
            Utf8NameOrValue utf8NameOrValue = new Utf8NameOrValue("ABCD");
            Assert.IsTrue(utf8NameOrValue.Value == "ABCD");

            byte[] buffer = utf8NameOrValue.ToByteArray();
            Assert.IsTrue(buffer[0] == 'A');
            Assert.IsTrue(buffer[1] == 'B');
            Assert.IsTrue(buffer[2] == 'C');
            Assert.IsTrue(buffer[3] == 'D');

            Utf8NameOrValue utf8Name2 = new Utf8NameOrValue(buffer);
            Assert.IsTrue(utf8Name2.Value == "ABCD");
            Assert.IsTrue(utf8Name2.RawValue is string);
        }
    }
}