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
namespace CassandraSharpUnitTests.Utils
{
    using System;
    using CassandraSharp.Utils;
    using Moq;
    using NUnit.Framework;

    [TestFixture]
    public class DisposableExtensionsTest
    {
        [Test]
        public void TestException()
        {
            Mock<IDisposable> mock = new Mock<IDisposable>();
            mock.Setup(x => x.Dispose()).Throws<ArithmeticException>();

            IDisposable disposable = mock.Object;
            disposable.SafeDispose();

            mock.Verify(x => x.Dispose(), Times.Once());
        }

        [Test]
        public void TestNormal()
        {
            Mock<IDisposable> mock = new Mock<IDisposable>();
            mock.Setup(x => x.Dispose());

            IDisposable disposable = mock.Object;
            disposable.SafeDispose();

            mock.Verify(x => x.Dispose(), Times.Once());
        }

        [Test]
        public void TestNull()
        {
            IDisposable disposable = null;
            disposable.SafeDispose();
        }
    }
}