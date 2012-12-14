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

namespace CassandraSharp.Utils
{
    using System;
    using System.Linq;
    using System.Reflection;

    internal static class ServiceActivator
    {
        public static string GetTypeName<T>()
        {
            return typeof(T).AssemblyQualifiedName;
        }

        public static T Create<T>(string customType, params object[] prms)
        {
            if (string.IsNullOrEmpty(customType))
            {
                return default(T);
            }

            Type type = Type.GetType(customType);
            if (null == type || !typeof(T).IsAssignableFrom(type))
            {
                string invalidTypeMsg = string.Format("'{0}' is not a valid type", customType);
                throw new ArgumentException(invalidTypeMsg);
            }

            // mini-dependency injection
            ConstructorInfo ci = type.GetConstructors().Single();
            ParameterInfo[] pis = ci.GetParameters();
            object[] ciPrms = new object[pis.Length];
            for (int idx = 0; idx < ciPrms.Length; ++idx)
            {
                Type piType = pis[idx].ParameterType;
                ciPrms[idx] = prms.First(piType.IsInstanceOfType);
            }

            return (T) Activator.CreateInstance(type, ciPrms);
        }
    }
}