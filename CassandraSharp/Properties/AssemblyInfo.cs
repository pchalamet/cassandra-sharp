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

using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.

[assembly: AssemblyTitle("CassandraSharp")]
[assembly: AssemblyDescription("cassandra-sharp is a .NET client for Apache Cassandra")]
#if DEBUG

[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyCompany("Pierre Chalamet")]
[assembly: AssemblyProduct("cassandra-sharp")]
[assembly: AssemblyCopyright("(c) 2011-2012 Pierre Chalamet")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.

[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM

[assembly: Guid("7eae01cf-4f32-4ab5-96f0-7b61ea8f285d")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the '*' as shown below:
// [assembly: AssemblyVersion("1.0.*")]

[assembly: InternalsVisibleTo("CassandraSharpUnitTests, PublicKey=0024000004800000940000000602000000240000525341310004000001000100f9405d912eeb97aa3ec5968c71608e75b2b672a918568a43345ea9c474999026643de7a44fc861de7f8fbccce1611114277975ac0f4ceb57a87616ad60d9e9805a9ff25fbee8a9fc406828d2b31fd1766373b62825616e00b621a5fc28ca74898ddf4bdef767abd2c804bea0d300a3f57a12371bef43b4e94f67cc5edbb50cad")]