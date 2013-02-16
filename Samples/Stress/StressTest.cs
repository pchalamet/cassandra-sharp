// cassandra-sharp - a .NET client for Apache Cassandra
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

namespace Samples.Stress
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;

    public class DisconnectingProxy
    {
        private readonly double _probaDisconnect;

        private readonly double _probaSlowness;

        private readonly int _source;

        private readonly int _target;

        private volatile bool _enableKiller;

        private volatile bool _stop;

        public DisconnectingProxy(int source, int target, double probaDisconnect, double probaSlowness)
        {
            _source = source;
            _target = target;
            _probaDisconnect = probaDisconnect;
            _probaSlowness = probaSlowness;
        }

        public void Start()
        {
            ThreadPool.QueueUserWorkItem(_ => Worker());
            Thread.Sleep(3000);
            Console.WriteLine("Proxy is started");
        }

        public void EnableKiller()
        {
            _enableKiller = true;
        }

        public void Stop()
        {
            _stop = true;
        }

        private void Worker()
        {
            IPHostEntry ipHostInfo = Dns.GetHostEntry("localhost");
            IPAddress ipAddress = ipHostInfo.AddressList.First(x => x.AddressFamily == AddressFamily.InterNetwork);
            EndPoint listenEndpoint = new IPEndPoint(ipAddress, _source);

            while (!_stop)
            {
                EndPoint targetEndpoint = new IPEndPoint(ipAddress, _target);
                Socket targetSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                targetSocket.ReceiveTimeout = 10000;
                targetSocket.SendTimeout = 10000;
                targetSocket.Connect(targetEndpoint);

                Socket listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                listenSocket.ReceiveTimeout = 10000;
                listenSocket.SendTimeout = 10000;
                listenSocket.Bind(listenEndpoint);
                listenSocket.Listen(10);

                Socket clientSocket = listenSocket.Accept();
                clientSocket.ReceiveTimeout = 10000;
                clientSocket.SendTimeout = 10000;
                ThreadPool.QueueUserWorkItem(_ => Transmit(clientSocket, targetSocket));
                ThreadPool.QueueUserWorkItem(_ => Transmit(targetSocket, clientSocket));
                Killer(targetSocket, listenSocket);
            }
        }

        private void Killer(params Socket[] sockets)
        {
            Random rnd = new Random();
            while (!_stop)
            {
                Thread.Sleep(rnd.Next(500));

                double proba = rnd.NextDouble();
                if (_enableKiller && proba < _probaDisconnect)
                {
                    break;
                }
            }

            if (!_stop)
            {
                Console.WriteLine("******* Killing connection");
            }

            foreach (Socket socket in sockets)
            {
                try
                {
                    // socket.Disconnect(false);
                    socket.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failure while shutdown {0}", ex);
                }
            }
        }

        private void Transmit(Socket source, Socket target)
        {
            Random rnd = new Random();
            try
            {
                byte[] buffer = new byte[1024];
                while (true)
                {
                    int count = source.Receive(buffer);
                    int write = 0;
                    while (write != count)
                    {
                        double proba = rnd.NextDouble();
                        if (_enableKiller && proba < _probaSlowness)
                        {
                            Console.WriteLine("******* Introducing slowness");
                            Thread.Sleep(100*1000);
                        }

                        write += target.Send(buffer, write, count - write, SocketFlags.None);
                    }
                }
            }
            catch
            {
            }
        }
    }

    public class ResilienceTest
    {
        public void RecoveryTest()
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig
                {
                        Recovery = new RecoveryConfig {Type = "Default", Interval = 2},
                        Logger = new LoggerConfig {Type = typeof(ResilienceLogger).AssemblyQualifiedName},
                };
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {"localhost"},
                            },
                        Transport = new TransportConfig
                            {
                                    Port = 666,
                                    Type = "ShortRunning",
                                    ReceiveTimeout = 10000,
                                    SendTimeout = 10000,
                            }
                };

            DisconnectingProxy proxy = new DisconnectingProxy(666, 9042, 0.05, 0.0);
            proxy.Start();

            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
            {
                ICqlCommand cmd = cluster.CreatePocoCommand();

                const string dropFoo = "drop keyspace data";
                try
                {
                    cmd.Execute(dropFoo).AsFuture().Wait();
                }
                catch
                {
                }

                const string createFoo = "CREATE KEYSPACE data WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                cmd.Execute(createFoo).AsFuture().Wait();

                const string createBar = "CREATE TABLE data.test (time text PRIMARY KEY)";
                cmd.Execute(createBar).AsFuture().Wait();

                proxy.EnableKiller();

                List<Task> tasks = new List<Task>();
                for (int taskId = 0; taskId < 5; ++taskId)
                {
                    int tmpTaskId = taskId;
                    Task task = Task.Factory.StartNew(() => Worker(tmpTaskId, cmd), TaskCreationOptions.LongRunning);
                    tasks.Add(task);
                }

                foreach (Task task in tasks)
                {
                    task.Wait();
                }

                ClusterManager.Shutdown();
            }

            proxy.Stop();
        }

        private static void Worker(int taskId, ICqlCommand cmd)
        {
            for (int reqId = 0; reqId < 10000; ++reqId)
            {
                int attempt = 0;
                while (true)
                {
                    try
                    {
                        var now = DateTime.Now;
                        string insert = String.Format("insert into data.test(time) values ('{0}');", now);
                        cmd.Execute(insert).AsFuture().Wait();
                        Console.WriteLine("Task {0} RequestId {1} Try {2} ==> {3}", taskId, reqId, ++attempt, insert);
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("FAILED Task {0} RequestId {1} Try {2} ==> {3}", taskId, reqId, ++attempt, ex.Message);
                        Thread.Sleep(5*1000);
                    }
                }
            }
        }

        public class ResilienceLogger : ILogger
        {
            public void Debug(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Info(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Warn(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Error(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public void Fatal(string format, params object[] prms)
            {
                Console.WriteLine(format, prms);
            }

            public bool IsDebugEnabled()
            {
                return true;
            }
        }
    }
}