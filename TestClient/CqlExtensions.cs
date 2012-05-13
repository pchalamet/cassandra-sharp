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
    using System.IO;
    using Apache.Cassandra;
    using CassandraSharp.MadeSimple;

    public static class CqlExtensions
    {
        public static void Dump(this CqlResult result, TextWriter txtWriter)
        {
            txtWriter.WriteLine("ResultType = {0}", result.Type);
            txtWriter.WriteLine();
            for (int i = 0; i < result.Rows.Count; ++i)
            {
                txtWriter.WriteLine("key = {0}", new Utf8NameOrValue(result.Rows[i].Key).Value);
                for (int j = 0; j < result.Rows[i].Columns.Count; ++j)
                {
                    Utf8NameOrValue column = new Utf8NameOrValue(result.Rows[i].Columns[j].Name);
                    txtWriter.Write("{0} = ", column.Value);
                    switch (column.Value)
                    {
                        case "firstname":
                            txtWriter.WriteLine("{0}", new Utf8NameOrValue(result.Rows[i].Columns[j].Value).Value);
                            break;

                        case "lastname":
                            txtWriter.WriteLine("{0}", new Utf8NameOrValue(result.Rows[i].Columns[j].Value).Value);
                            break;

                        case "birthyear":
                            txtWriter.WriteLine("{0}", new IntNameOrValue(result.Rows[i].Columns[j].Value).Value);
                            break;
                    }
                }
                txtWriter.WriteLine();
            }
        }
    }
}