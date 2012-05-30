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

namespace CassandraSharp.ObjectMapper
{
    using System;
    using System.Collections.Generic;

    public enum RowCacheProvider
    {
        SerializingCacheProvider,

        ConcurrentHashMapCacheProvider
    }

    public enum CompactionStrategy
    {
        SizeTieredCompactionStrategy,

        LeveledCompactionStrategy
    }

    [AttributeUsage(AttributeTargets.Class)]
    public class SchemaAttribute : Attribute
    {
//compaction_strategy_class	SizeTieredCompactionStrategy
//compaction_strategy_options	none
//compression_options	none
//comparator	text
//comment	‘’(an empty string)
//dc_local_read_repair_chance	0.0
//default_validation	text
//gc_grace_seconds	864000
//min_compaction_threshold	4
//max_compaction_threshold	32
//read_repair_chance	1.0
//replicate_on_write

        public SchemaAttribute(string keyspace)
        {
            Keyspace = keyspace;
        }

        public string Keyspace { get; private set; }

        public string Name { get; set; }

        // public Type Comparator { get; set; }
        public string Comment { get; set; }

        public bool CompactStorage { get; set; }

        //public Type DefaultValidation { get; set; }

        //public int? GCGraceSeconds { get; set; }

        //public int? MinCompactionThreshold { get; set; }

        //public int? MaxCompactionThreshold { get; set; }

        //public double? ReadRepairChance { get; set; }

        //public int? ReplicateOnWrite { get; set; }

        //public RowCacheProvider RowCacheProvider { get; set; }

        //public int? RowCacheSize { get; set; }

        //public int? KeyCacheSize { get; set; }

        //public int? RowCacheSavePeriodInSeconds { get; set; }

        //public int? KeyCacheSavePeriodInSeconds { get; set; }
    }
}