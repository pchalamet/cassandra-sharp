// cassandra-sharp - high performance .NET driver for Apache Cassandra
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

namespace Samples.PropertyBag
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp;
    using CassandraSharp.CQLPropertyBag;

    public class SchemaColumns
    {
        public string KeyspaceName { get; set; }

        public string ColumnFamilyName { get; set; }

        public string ColumnName { get; set; }

        public int ComponentIndex { get; set; }

        public string Validator { get; set; }
    }

    public class PropertyBagSample : Sample
    {
        public PropertyBagSample()
            : base("PropertyBag")
        {
        }

        protected override void InternalRun(ICluster cluster)
        {
            var cmd = cluster.CreatePropertyBagCommand();

            const string cqlKeyspaces = "SELECT * from system_schema.columns";

            var req = from t in cmd.Execute(cqlKeyspaces).AsFuture().Result
                      where "system" == (string) t["KeyspaceName"]
                      select t;
            DisplayResult(req);
        }

        private static void DisplayResult(IEnumerable<PropertyBag> reqs)
        {
            foreach (PropertyBag pb in reqs)
            {
                Console.WriteLine("KeyspaceName={0} TableName={1} ColumnName={2}",
                                  pb["KeyspaceName"], pb["TableName"], pb["ColumnName"]);
            }
        }
    }
}