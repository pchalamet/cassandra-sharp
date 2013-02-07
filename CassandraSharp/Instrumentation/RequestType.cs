using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CassandraSharp.Instrumentation
{
    public enum RequestType
    {
        Prepare, Authenticate, Ready, Query
    }
}
