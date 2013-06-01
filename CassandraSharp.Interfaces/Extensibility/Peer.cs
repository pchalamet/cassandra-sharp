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

namespace CassandraSharp.Extensibility
{
    using System;
    using System.Net;
    using System.Numerics;
    using System.Linq;

    public sealed class Peer
    {
        public Peer()
        {
        }


        public Peer(IPAddress rpcAddress, string datacenter, string rack, BigInteger[] tokens)
            : this()
        {
            this.RpcAddress = rpcAddress;
            this.Datacenter = datacenter;
            this.Rack = rack;
            this.Tokens = tokens;
        }

        public IPAddress RpcAddress { get; internal set; }

        public string Datacenter { get; internal set; }

        public string Rack { get; internal set; }

        public BigInteger[] Tokens { get; internal set; }


        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            var item = obj as Peer;
            if ((object)item == null)
                return false;

            return item.RpcAddress.Equals(this.RpcAddress)
                && (item.Tokens.Length == this.Tokens.Length && item.Tokens.Intersect(this.Tokens).Count() == item.Tokens.Length)
                && item.Datacenter.Equals(this.Datacenter, StringComparison.InvariantCulture)
                && item.Rack.Equals(this.Rack, StringComparison.InvariantCulture);
        }

        public override int GetHashCode()
        {
            //use built in anonymous hashing
            return new
            {
                this.RpcAddress,
                Datacentre = this.Datacenter,
                this.Rack,
                this.Tokens
            }.GetHashCode();
        }

        /// <summary>
        /// Gets a proximity score where:
        /// 0 = Same rack & dc
        /// 1 = Same dc
        /// 2 = Different dc
        /// </summary>
        /// <param name="peer"></param>
        /// <returns></returns>
        public int GetProximity(Peer peer)
        {
            if (this.Datacenter.Equals(peer.Datacenter, StringComparison.InvariantCulture)
                && this.Rack.Equals(peer.Rack, StringComparison.InvariantCulture))
                return 0; //same rack in same dc

            if (this.Datacenter == peer.Datacenter)
                //same dc differnt rack
                return 1;
            else
                //different dc, rack irrelevent
                return 2;
        }
    }
}