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

namespace cqlplus
{
    internal class CliArgs
    {
        [Argument(ArgumentType.AtMostOnce, HelpText = "Display help on startup", ShortName = "nohelp")]
        public bool NoHelp = false;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Do not check connection", ShortName = "chkcn")]
        public bool CheckConnection = true;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Input file", ShortName = "f")]
        public string File = null;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Hostname", ShortName = "h")]
        public string Hostname = "localhost";

        [Argument(ArgumentType.AtMostOnce, HelpText = "Password", ShortName = "x")]
        public string Password = null;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Port", ShortName = "p")]
        public int Port = 9042;

        [Argument(ArgumentType.AtMostOnce, HelpText = "User", ShortName = "u")]
        public string User = null;

        [Argument(ArgumentType.AtMostOnce, HelpText = "Enable debug log", ShortName = "dbglog")]
        public bool DebugLog = false;
    }
}