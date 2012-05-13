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
    using System;
    using CassandraSharp.NameOrValues;
    using NUnit.Framework;

    [TestFixture]
    public class TimeUuidNameOrValueTest
    {
        [Test]
        public void TestNonUtcDateTime()
        {
            DateTime now = DateTime.Now;

            Assert.Throws<ApplicationException>(() => new TimeUuidNameOrValue(now));
        }

        [Test]
        public void TestTimeUuidName()
        {
            DateTime now = DateTime.Now.ToUniversalTime();

            TimeUuidNameOrValue timeUuidNameOrValue = new TimeUuidNameOrValue(now);
            Assert.IsTrue(timeUuidNameOrValue.Value == now);

            byte[] buffer = timeUuidNameOrValue.ToByteArray();

            TimeUuidNameOrValue timeUuidName2 = new TimeUuidNameOrValue(buffer);
            Assert.IsTrue(timeUuidName2.Value == now);

            Assert.IsTrue(timeUuidName2.RawValue is DateTime);
        }
    }
}