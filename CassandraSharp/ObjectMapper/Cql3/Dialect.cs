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

namespace CassandraSharp.ObjectMapper.Cql3
{
    using CassandraSharp.ObjectMapper.Dialect;

    public class Dialect : ICqlDialect
    {
        public ICreateTableBuilder GetCreateTableBuilder()
        {
            return new CreateTableBuilder();
        }

        public IDropTableBuilder GetDropTableBuilder()
        {
            return new DropTableBuilder();
        }

        public ITruncateTableBuilder GetTruncateTableBuilder()
        {
            return new TruncateTableBuilder();
        }

        public IInsertBuilder GetInsertBuilder()
        {
            return new InsertBuilder();
        }

        public IUpdateBuilder GetUpdateBuilder()
        {
            return new UpdateBuilder();
        }

        public IQueryBuilder GetQueryBuilder()
        {
            return new QueryBuilder();
        }
    }
}