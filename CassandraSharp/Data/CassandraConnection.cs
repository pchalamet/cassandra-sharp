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
    using CassandraSharp.Config;
    using CassandraSharp.Utils;

    public class CassandraConnection : IDbConnection
    {
        private ICluster _realCluster;

        internal ICluster CurrentCluster { get; set; }

        public void Dispose()
        {
            Close();
        }

        public IDbTransaction BeginTransaction()
        {
            throw new NotSupportedException();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            throw new NotSupportedException();
        }

        public void Close()
        {
            CurrentCluster.SafeDispose();
            CurrentCluster = null;

            _realCluster.SafeDispose();
            _realCluster = null;
        }

        public void ChangeDatabase(string databaseName)
        {
            Database = databaseName;
            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = databaseName};
            CurrentCluster = _realCluster.CreateChildCluster(cfgBuilder);
        }

        public IDbCommand CreateCommand()
        {
            return new CassandraCommand(this);
        }

        public void Open()
        {
            _realCluster = ClusterManager.GetCluster(ConnectionString);
            CurrentCluster = _realCluster;
            Database = _realCluster.BehaviorConfig.KeySpace;
        }

        public string ConnectionString { get; set; }

        public int ConnectionTimeout
        {
            get { throw new NotSupportedException(); }
        }

        public string Database { get; private set; }

        public ConnectionState State
        {
            get
            {
                return null != _realCluster
                           ? ConnectionState.Open
                           : ConnectionState.Closed;
            }
        }
    }
}