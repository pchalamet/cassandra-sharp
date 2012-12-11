ABOUT
cassandra-sharp is a .NET 4+ client for Apache Cassandra released under the Apache License 2.0.
It is able to interact with the Cassandra 1.2+ using CQL 3.0 only.
cassandra-sharp supports async operations (thanks to TPL tasks) - both streaming and future are supported.
cassandra-sharp does support reliable error handling (and reporting) during async operations.

HOW TO BUILD
To compile, use a command line and run the following commands:
  GenerateVersion.cmd <flavor> <VersionNumber> <VersionStatus>
NOTE: type "GenerateVersion.cmd /?" for more info.
  
You will find a package named "cassandra-sharp-bin-<VersionNumber>.zip" in the OutDir folder.

THIRD PARTIES
This projects relies on the following third parties:
* MSBuild Community Tasks Project (http://msbuildtasks.tigris.org/)
  released under BSD License (http://opensource.org/licenses/bsd-license.php)
* Moq (http://code.google.com/p/moq/)
  released under New BSD License
* NUnit (http://www.nunit.org/)
  released NUnit license
* NuGet (http://nuget.org/)
  released under Apache License 2.0
* Command Line Argument Parser Library (http://commandlinearguments.codeplex.com/)
  released under Microsoft Public License (Ms-PL)