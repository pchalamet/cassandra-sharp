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

namespace TestClient
{
    using System;
    using System.Collections.Generic;
    using Apache.Cassandra;
    using CassandraSharp;
    using CassandraSharp.MadeSimple;

    public class MadeSimpleSample
    {
        private readonly string _configName;

        public MadeSimpleSample(string configName)
        {
            _configName = configName;
        }

        public void Run()
        {
            using (ICluster cluster = ClusterManager.GetCluster(_configName))
                Run(cluster);
        }

        protected virtual void Run(ICluster cluster)
        {
            // initialize schema
            try
            {
                cluster.ExecuteCql("drop table People");
            }
            catch
            {
            }
            cluster.ExecuteCql("create table People (firstname text primary key) with default_validation=blob");

            // insert data
            List<byte[]> prms;
            CqlResult result;

            Utf8NameOrValue insert = new Utf8NameOrValue("insert into People (firstname, lastname, birthyear) values (?, ?, ?)");
            CqlPreparedResult preparedInsert = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.prepare_cql_query(insert.ToByteArray(), Compression.NONE));

            Utf8NameOrValue firstName = new Utf8NameOrValue("pierre");
            Utf8NameOrValue lastName = new Utf8NameOrValue("chalamet");
            IntNameOrValue birthyear = new IntNameOrValue(1973);

            prms = new List<byte[]> {firstName.ToByteArray(), lastName.ToByteArray(), birthyear.ToByteArray()};
            result = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.execute_prepared_cql_query(preparedInsert.ItemId, prms));

            prms = new List<byte[]> {new Utf8NameOrValue("isabelle").ToByteArray(), lastName.ToByteArray(), new IntNameOrValue(1972).ToByteArray()};
            result = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.execute_prepared_cql_query(preparedInsert.ItemId, prms));

            //result = cluster.ExecuteCql("select * from People where firstname in (?)");
            //DumpCqlResult(result);

            // query data
            Utf8NameOrValue select = new Utf8NameOrValue("select lastname, birthyear from People where firstname in (?, ?)");
            CqlPreparedResult preparedSelect = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.prepare_cql_query(select.ToByteArray(), Compression.NONE));

            prms = new List<byte[]> {firstName.ToByteArray(), new Utf8NameOrValue("isabelle").ToByteArray()};
            result = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.execute_prepared_cql_query(preparedSelect.ItemId, prms));
            result.Dump(Console.Out);
        }
    }
}