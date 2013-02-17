// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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

namespace cqlplus.Commands
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    internal class ShellCommand : CommandBase
    {
        private static readonly Dictionary<string, Type> _registeredCommands = new Dictionary<string, Type>
            {
                    {"help", typeof(Help)},
                    {"set", typeof(Set)},
                    {"exit", typeof(Exit)},
                    {"reset", typeof(Reset)},
                    {"cls", typeof(ClearScreen)},
                    {"exec", typeof(Exec)},
                    {"source", typeof(Source)},
            };

        private readonly string _name;

        private readonly Dictionary<string, string> _parameters;

        public ShellCommand(string name, Dictionary<string, string> parameters)
        {
            _name = name;
            _parameters = parameters;
        }

        public override void Execute()
        {
            Type cmdType;
            if (!_registeredCommands.TryGetValue(_name, out cmdType))
            {
                throw new ArgumentException(string.Format("Unknown command {0}", _name));
            }

            ICommand cmd = (ICommand) Activator.CreateInstance(cmdType);

            // feed parameters
            const BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.SetProperty;
            foreach (KeyValuePair<string, string> prm in _parameters)
            {
                PropertyInfo pi = cmd.GetType().GetProperty(prm.Key, bindingFlags);
                if (null == pi)
                {
                    throw new ArgumentException(string.Format("Unknown parameter {0}", prm.Key));
                }

                try
                {
                    object value = ParseValue(pi.PropertyType, prm.Value);
                    pi.SetValue(cmd, value, null);
                }
                catch
                {
                    throw new ArgumentException(string.Format("Invalid argument type for parameter {0}", prm.Key));
                }
            }

            foreach (PropertyInfo pi in cmd.GetType().GetProperties(bindingFlags))
            {
                string piName = pi.Name.ToLower();
                DescriptionAttribute prmAttribute =
                        (DescriptionAttribute) pi.GetCustomAttributes(typeof(DescriptionAttribute), true).SingleOrDefault();
                if (null != prmAttribute && prmAttribute.Mandatory)
                {
                    if (! _parameters.ContainsKey(piName))
                    {
                        throw new ArgumentException(string.Format("Parameter {0} is missing", pi.Name));
                    }
                }
            }

            cmd.Validate();
            cmd.Execute();
        }

        private object ParseValue(Type propertyType, string prmValue)
        {
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                propertyType = propertyType.GetGenericArguments()[0];
            }

            object value = prmValue;
            if (propertyType.IsEnum)
            {
                value = Enum.Parse(propertyType, prmValue, true);
            }
            else if (propertyType != typeof(string))
            {
                MethodInfo mi = propertyType.GetMethod("Parse", new[] {typeof(string)});
                value = mi.Invoke(null, new object[] {prmValue});
            }

            return value;
        }

        public static IEnumerable<KeyValuePair<string, Type>> GetRegisteredCommands()
        {
            return _registeredCommands;
        }
    }
}