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
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using CassandraSharp;

    [Description("display help")]
    internal class Help : CommandBase
    {
        public override void Execute()
        {
            int cmdMaxLen = 0;
            foreach (var cmdType in ShellCommand.GetRegisteredCommands())
            {
                cmdMaxLen = Math.Max(cmdMaxLen, cmdType.Key.Length);
            }

            string version = typeof(ClusterManager).Assembly.GetName().Version.ToString();
            Console.WriteLine("Using cassandra-sharp v{0}", version);
            Console.WriteLine();

            Console.WriteLine("Commands:");
            string format = string.Format("  !{{0,-{0}}} - ", cmdMaxLen);
            foreach (var cmdType in ShellCommand.GetRegisteredCommands())
            {
                ICommand cmd = (ICommand) Activator.CreateInstance(cmdType.Value);
                DescriptionAttribute cmdAttribute =
                        (DescriptionAttribute) cmdType.Value.GetCustomAttributes(typeof(DescriptionAttribute), true).SingleOrDefault();
                string cmdDescription = null != cmdAttribute
                                                ? cmdAttribute.Description
                                                : "";
                string startOfLine = string.Format(format, cmdType.Key);
                string nextStartOfLine = new string(' ', startOfLine.Length);
                string[] lines = cmdDescription.Split(new[] {"\n"}, StringSplitOptions.RemoveEmptyEntries);
                foreach (string line in lines)
                {
                    Console.WriteLine("{0}{1}", startOfLine, line);
                    startOfLine = nextStartOfLine;
                }

                foreach (PropertyInfo propertyInfo in cmd.GetType().GetProperties())
                {
                    string friendlyType = GetFriendlyPropertyType(propertyInfo);
                    DescriptionAttribute prmAttribute =
                            (DescriptionAttribute) propertyInfo.GetCustomAttributes(typeof(DescriptionAttribute), true).SingleOrDefault();
                    string prmDescription = "";
                    string beforeType = "[";
                    string afterType = "]";
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
            Type type = propertyInfo.PropertyType;
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                type = type.GetGenericArguments()[0];
            }

            if (type.IsEnum)
            {
                string[] enumValues = Enum.GetNames(type);
                StringBuilder sbEnums = new StringBuilder();
                string sep = "";
                foreach (string enumValue in enumValues)
                {
                    sbEnums.Append(sep).Append(enumValue);
                    sep = ",";
                }
                return sbEnums.ToString();
            }

            if (type == typeof(int))
            {
                return "int";
            }
            if (type == typeof(string))
            {
                return "string";
            }
            if (type == typeof(bool))
            {
                return "bool";
            }
            return type.Name;
        }
    }
}