namespace cqlsh
{
    using CommandLine;

    internal class CliArgs
    {
        [Argument(ArgumentType.AtMostOnce, HelpText = "Hostname", ShortName = "h", DefaultValue = "localhost")]
// ReSharper disable InconsistentNaming
        public string hostname = null;
// ReSharper restore InconsistentNaming

        [Argument(ArgumentType.AtMostOnce, HelpText = "Input file", ShortName = "f")]
// ReSharper disable InconsistentNaming
        public string file = null;
// ReSharper restore InconsistentNaming
    }
}