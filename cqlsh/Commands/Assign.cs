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

    internal class Assign : ICommand
    {
        private readonly object _identifier;

        private readonly object _value;

        public Assign(object identifier, object value)
        {
            _identifier = identifier;
            _value = value;
        }

        public void Execute()
        {
            string identifier = (string) _identifier;
            switch (identifier)
            {
                case "colwidth":
                    CommandContext.Instance.ColumnWidth = (int) _value;
                    break;

                case "tab":
                    CommandContext.Instance.Tabular = (bool) _value;
                    break;

                default:
                    throw new ArgumentException(string.Format("Unknown property {0}", identifier));
            }
        }
    }
}