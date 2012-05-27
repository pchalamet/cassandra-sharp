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
    using CassandraSharp;
    using CassandraSharp.ObjectMapper;

    //CREATE TABLE seen_ships (
    //      day text,
    //      time_seen timestamp,
    //      shipname text,
    //      PRIMARY KEY (day, time_seen)
    //  );

    [Schema("ObjectMapper", Comment = "Captain Reynolds register", Name = "seen_ships", CompactStorage = true)]
    public class SeenShips
    {
        [Key(Name = "day")]
        public string Day;

        [Column(Name = "shipname")]
        public string ShipName;

        [CompositeKey(Name = "time_seen", Index = 1)]
        public DateTime? TimeSeen;
    }

    public class ObjectMapperSample
    {
        private readonly string _configName;

        public ObjectMapperSample(string configName)
        {
            _configName = configName;
        }

        public void Run()
        {
            using (ICluster cluster = ClusterManager.GetCluster(_configName))
                Run(cluster);
        }

        protected void Run(ICluster cluster)
        {
            cluster.CreateTable<SeenShips>();

            SeenShips seenShip = new SeenShips
                                     {
                                         Day = "199-A/4",
                                         TimeSeen = new DateTime(1973, 06, 19),
                                         ShipName = "Sunrise Avenger"
                                     };
            cluster.Insert(seenShip);

            seenShip = new SeenShips
                           {
                               Day = "199-A/4",
                               TimeSeen = new DateTime(1973, 06, 20),
                               ShipName = "Sunrise Avenger2"
                           };
            cluster.Insert(seenShip);

            seenShip = new SeenShips
                           {
                               Day = "199-A/5",
                               TimeSeen = new DateTime(1973, 06, 21),
                               ShipName = "Sunrise Avenger3"
                           };
            cluster.Insert(seenShip);

            SeenShips queryShips = new SeenShips
                                       {
                                           Day = "199-A/4"
                                       };
            IEnumerable<SeenShips> seenShips = cluster.Select(queryShips);
            foreach (SeenShips ss in seenShips)
            {
                Console.WriteLine("{0}: {1}", ss.TimeSeen, ss.ShipName);
            }

            cluster.DropTable<SeenShips>();
        }
    }
}