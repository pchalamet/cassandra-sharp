namespace CassandraSharp.ObjectMapper
{
    using System;

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