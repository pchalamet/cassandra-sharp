// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using System;
using System.Linq;
using System.Reflection;
using System.Text;

namespace cqlplus.Commands
{
    [Description("display help")]
    internal class Help : CommandBase
    {
        public override void Execute()
        {
            var cmdMaxLen = 0;
            foreach (var cmdType in ShellCommand.GetRegisteredCommands()) cmdMaxLen = Math.Max(cmdMaxLen, cmdType.Key.Length);

            var version = CommandContext.Cluster.GetType().Assembly.GetName().Version.ToString();
            Console.WriteLine("cassandra-sharp {0}", version);
            Console.WriteLine("OS {0} / .NET {1}", Environment.OSVersion, Environment.Version);
            Console.WriteLine("OS x64 {0} / Process x64 {1} / Proc count {2}", Environment.Is64BitOperatingSystem, Environment.Is64BitProcess,
                              Environment.ProcessorCount);
            Console.WriteLine();

            Console.WriteLine("Commands:");
            var format = string.Format("  !{{0,-{0}}} - ", cmdMaxLen);
            foreach (var cmdType in ShellCommand.GetRegisteredCommands())
            {
                var cmd = (ICommand)Activator.CreateInstance(cmdType.Value);
                var cmdAttribute =
                    (DescriptionAttribute)cmdType.Value.GetCustomAttributes(typeof(DescriptionAttribute), true).SingleOrDefault();
                var cmdDescription = null != cmdAttribute
                                         ? cmdAttribute.Description
                                         : "";
                var startOfLine = string.Format(format, cmdType.Key);
                var nextStartOfLine = new string(' ', startOfLine.Length);
                var lines = cmdDescription.Split(new[] {"\n"}, StringSplitOptions.RemoveEmptyEntries);
                foreach (var line in lines)
                {
                    Console.WriteLine("{0}{1}", startOfLine, line);
                    startOfLine = nextStartOfLine;
                }

                foreach (var propertyInfo in cmd.GetType().GetProperties())
                {
                    var friendlyType = GetFriendlyPropertyType(propertyInfo);
                    var prmAttribute =
                        (DescriptionAttribute)propertyInfo.GetCustomAttributes(typeof(DescriptionAttribute), true).SingleOrDefault();
                    var prmDescription = "";
                    var beforeType = "[";
                    var afterType = "]";
                    if (null != prmAttribute)
                    {
                        prmDescription = prmAttribute.Description;
                        if (prmAttribute.Mandatory)
                        {
                            beforeType = "<";
                            afterType = ">";
                        }
                    }

                    Console.WriteLine("{0}  -> {1}={2}{3}{4} : {5}", startOfLine, propertyInfo.Name, beforeType, friendlyType, afterType, prmDescription);
                    startOfLine = nextStartOfLine;
                }
            }

            Console.WriteLine("  CQL query");

            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  !set log=true colwidth=20;");
            Console.WriteLine("  select * from system.local;");
        }

        private static string GetFriendlyPropertyType(PropertyInfo propertyInfo)
        {
            var type = propertyInfo.PropertyType;
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>)) type = type.GetGenericArguments()[0];

            if (type.IsEnum)
            {
                var enumValues = Enum.GetNames(type);
                var sbEnums = new StringBuilder();
                var sep = "";
                foreach (var enumValue in enumValues)
                {
                    sbEnums.Append(sep).Append(enumValue);
                    sep = ",";
                }

                return sbEnums.ToString();
            }

            if (type == typeof(int)) return "int";
            if (type == typeof(string)) return "string";
            if (type == typeof(bool)) return "bool";
            return type.Name;
        }
    }
}