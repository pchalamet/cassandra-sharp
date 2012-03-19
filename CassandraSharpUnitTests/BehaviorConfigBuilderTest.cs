namespace CassandraSharpUnitTests
{
    using System;
    using System.Reflection;
    using CassandraSharp;
    using CassandraSharp.Config;
    using NUnit.Framework;

    [TestFixture]
    public class BehaviorConfigBuilderTest
    {
        // ensure that BehaviorConfigBuilder implements all required properties
        // to build a BehaviorConfig instance
        [Test]
        public void MustHaveSamePropertiesThanBehaviorConfigButWithNullable()
        {
            Type behaviorConfigBuilderType = typeof(BehaviorConfigBuilder);

            foreach (PropertyInfo propertyInfo in typeof(BehaviorConfig).GetProperties())
            {
                PropertyInfo proxyPropInfo = behaviorConfigBuilderType.GetProperty(propertyInfo.Name);

                Assert.IsNotNull(proxyPropInfo,
                                 "BehaviorConfigBuilder must implement property {0}",
                                 propertyInfo.Name);

                Type proxyType = propertyInfo.PropertyType.IsClass
                                     ? propertyInfo.PropertyType
                                     : typeof(Nullable<>).MakeGenericType(propertyInfo.PropertyType);
                Assert.IsTrue(proxyType == proxyPropInfo.PropertyType,
                              "BehaviorConfigBuilder must implement property {0} with type {1}",
                              propertyInfo.Name, proxyType);
            }
        }
    }
}