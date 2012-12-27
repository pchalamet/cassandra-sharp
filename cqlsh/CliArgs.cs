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

namespace cqlsh
{
    internal class CliArgs
    {
        [Argument(ArgumentType.AtMostOnce, HelpText = "Display banner", ShortName = "b")]
        public bool Banner = true;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Do not check connection", ShortName = "cn")]
        public bool CheckConnection = true;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Input file", ShortName = "f")]
        public string File = null;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Hostname", ShortName = "h", DefaultValue = "localhost")]
        public string Hostname = null;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Password", ShortName = "p")]
        public string Password = null;

        [Argument(ArgumentType.AtMostOnce, HelpText = "User", ShortName = "u")]
        public string User = null;
    }
}