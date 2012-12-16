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

namespace cqlsh.Commands
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    internal class GenericCommand : ICommand
    {
        private static readonly Dictionary<string, Type> _registeredCommands = new Dictionary<string, Type>
            {
                    {"help", typeof(Help)},
                    {"set", typeof(Set)},
                    {"exit", typeof(Exit)},
                    {"reset", typeof(Reset)},
                    {"cls", typeof(ClearScreen)},
            };

        private readonly string _name;

        private readonly KeyValuePair<string, object>[] _parameters;

        public GenericCommand(string name, KeyValuePair<string, object>[] parameters)
        {
            _name = name;
            _parameters = parameters;
        }

        public string Describe()
        {
            throw new InvalidOperationException("GenericCommand.Describe should not be called");
        }

        public void Execute()
        {
            Type cmdType;
            if (!_registeredCommands.TryGetValue(_name, out cmdType))
            {
                throw new ArgumentException(string.Format("Unknown command {0}", _name));
            }

            ICommand cmd = (ICommand) Activator.CreateInstance(cmdType);

            // feed parameters
            const BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.SetProperty;
            foreach (KeyValuePair<string, object> prm in _parameters)
            {
                PropertyInfo pi = cmd.GetType().GetProperty(prm.Key, bindingFlags);
                if (null == pi)
                {
                    throw new ArgumentException(string.Format("Unknown parameter {0}", prm.Key));
                }

                try
                {
                    pi.SetValue(cmd, prm.Value, null);
                }
                catch
                {
                    throw new ArgumentException(string.Format("Invalid argument type for parameter {0}", prm.Key));
                }
            }

            cmd.Execute();
        }

        public static IEnumerable<KeyValuePair<string, Type>> GetRegisteredCommands()
        {
            return _registeredCommands;
        }
    }
}