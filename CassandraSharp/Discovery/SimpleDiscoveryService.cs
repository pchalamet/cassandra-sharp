//// cassandra-sharp - a .NET client for Apache Cassandra
//// Copyright (c) 2011-2012 Pierre Chalamet
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

//namespace CassandraSharp.Discovery
//{
//    using System;
//    using System.Timers;
//    using CassandraSharp.Extensibility;
//    using CassandraSharp.Utils;

//    internal class SimpleDiscoveryService : IDiscoveryService
//    {
//        private readonly ICluster _connectionFactory;

//        private readonly IEndpointStrategy _endpointStrategy;

//        private readonly Timer _timer;

//        public SimpleDiscoveryService(ICluster connectionFactory, IEndpointStrategy endpointStrategy)
//        {
//            _connectionFactory = connectionFactory;
//            _endpointStrategy = endpointStrategy;

//            // force an update immediately
//            TryDiscover();

//            _timer = new Timer(60*60*1000); // 1 hour
//            _timer.Elapsed += (s, e) => TryDiscover();
//            _timer.Enabled = true;

//            throw new NotImplementedException();
//        }

//        public void Dispose()
//        {
//            _timer.SafeDispose();
//        }

//        private void TryDiscover()
//        {
//            throw new NotImplementedException();

////            try
////            {
////                using (IConnection connection = _connectionFactory.GetConnection(null))
////                {
////                    //UNDONE
////                    //Dictionary<string, string> token2srv = connection.Transport.describe_token_map();
////                    //IEnumerable<IPAddress> servers = token2srv.Values.Select(NetworkFinder.Find);
////                    //_endpointStrategy.Update(servers);
////                }
////            }
////// ReSharper disable EmptyGeneralCatchClause
////            catch
////// ReSharper restore EmptyGeneralCatchClause
////            {
////            }
//        }
//    }
//}