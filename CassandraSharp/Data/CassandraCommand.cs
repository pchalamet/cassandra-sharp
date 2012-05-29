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

namespace CassandraSharp.Data
{
    using System;
    using System.Data;

    public class CassandraCommand : IDbCommand
    {
        private readonly CassandraDataParameterCollection _parameters = new CassandraDataParameterCollection();

        private CassandraConnection _connection;

        public CassandraCommand(CassandraConnection connection)
        {
            _connection = connection;
        }

        public void Dispose()
        {
            _connection = null;
        }

        public void Prepare()
        {
            throw new NotImplementedException();
        }

        public void Cancel()
        {
            throw new NotSupportedException();
        }

        public IDbDataParameter CreateParameter()
        {
            return new CassandraParameter();
        }

        public int ExecuteNonQuery()
        {
            throw new NotImplementedException();
        }

        public IDataReader ExecuteReader()
        {
            throw new NotImplementedException();
        }

        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            throw new NotImplementedException();
        }

        public object ExecuteScalar()
        {
            throw new NotImplementedException();
        }

        public IDbConnection Connection
        {
            get { return _connection; }
            set { _connection = (CassandraConnection) value; }
        }

        public IDbTransaction Transaction
        {
            get { return null; }
            set { throw new NotSupportedException(); }
        }

        public string CommandText { get; set; }

        public int CommandTimeout { get; set; }

        public CommandType CommandType
        {
            get { return CommandType.Text; }
            set { throw new NotSupportedException(); }
        }

        public IDataParameterCollection Parameters
        {
            get { return _parameters; }
        }

        public UpdateRowSource UpdatedRowSource
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }
    }
}