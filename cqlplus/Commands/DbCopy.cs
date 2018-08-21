//// cassandra-sharp - high performance .NET driver for Apache Cassandra
//// Copyright (c) 2011-2018 Pierre Chalamet
//// 
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
//// 
//// http://www.apache.org/licenses/LICENSE-2.0
//// 
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//namespace cqlplus.Commands
//{
//    using System;
//    using System.Data;
//    using System.Data.OleDb;
//    using System.Text;
//    using CassandraSharp;
//    using CassandraSharp.CQLPropertyBag;

//    [Description("select and insert data into a Cassandra table")]
//    internal class DbCopy : CommandBase
//    {
//        [Description("Select query", Mandatory = true)]
//        public string Command { get; set; }

//        [Description("OLEDB connection string to datasource", Mandatory = true)]
//        public string Db { get; set; }

//        [Description("Target keyspace", Mandatory = true)]
//        public string Keyspace { get; set; }

//        [Description("Target table", Mandatory = true)]
//        public string Table { get; set; }

//        [Description("Consistency level", Mandatory = true)]
//        public ConsistencyLevel CL { get; set; }

//        public override void Execute()
//        {
//            using (IDbConnection cnx = new OleDbConnection(Db))
//            {
//                cnx.Open();

//                using (IDbCommand cmd = cnx.CreateCommand())
//                {
//                    cmd.CommandText = Command;
//                    cmd.CommandType = CommandType.Text;

//                    using (IDataReader reader = cmd.ExecuteReader())
//                    {
//                        bool hasRes = reader.Read();
//                        if (! hasRes)
//                        {
//                            Console.WriteLine("Warning: no data found");
//                            return;
//                        }

//                        // create the insert query first
//                        using (var insertQuery = BuildInsertQuery(reader))
//                        {
//                            // iterate on records
//                            int count = 0;
//                            do
//                            {
//                                var bag = BuildPropertyBag(reader);

//                                ++count;
//                                insertQuery.Execute(bag).AsFuture();
//                            } while (reader.Read());

//                            Console.WriteLine("{0} record inserted", count);
//                        }
//                    }
//                }
//            }
//        }

//        private static PropertyBag BuildPropertyBag(IDataReader reader)
//        {
//            PropertyBag bag = new PropertyBag();
//            for (int i = 0; i < reader.FieldCount; ++i)
//            {
//                string name = reader.GetName(i);
//                object value = reader[i];
//                bag[name] = value;
//            }
//            return bag;
//        }

//        private IPreparedQuery<PropertyBag> BuildInsertQuery(IDataReader reader)
//        {
//            StringBuilder sbCols = new StringBuilder();
//            StringBuilder sbJokers = new StringBuilder();
//            string sep = "";
//            for (int i = 0; i < reader.FieldCount; ++i)
//            {
//                string name = reader.GetName(i);
//                sbCols.AppendFormat("{0}{1}", sep, name);
//                sbJokers.AppendFormat("{0}?", sep);
//                sep = ",";
//            }

//            string cqlInsert = string.Format("insert into {0}.{1} ({2}) values ({3})", Keyspace, Table, sbCols, sbJokers);
//            var ccmd = CommandContext.Cluster.CreatePropertyBagCommand().WithConsistencyLevel(CL);
//            IPreparedQuery<PropertyBag> insertQuery = ccmd.Prepare(cqlInsert);
//            return insertQuery;
//        }
//    }
//}