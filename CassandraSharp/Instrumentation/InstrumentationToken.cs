using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CassandraSharp.Instrumentation
{
    public class InstrumentationToken
    {
        public readonly Guid Id;
        public readonly RequestType Type;
        public readonly string Cql;

        private InstrumentationToken(Guid id, RequestType type, string cql)
        {
            this.Id = id;
            this.Type = type;
            this.Cql = cql;
        }

        internal static InstrumentationToken NewQueryToken(string cql)
        {
            return new InstrumentationToken(Guid.NewGuid(), RequestType.Query, cql);
        }

        internal static InstrumentationToken NewNonQueryToken(RequestType requestType)
        {
            return new InstrumentationToken(Guid.NewGuid(), requestType, null);
        }
    }
}
