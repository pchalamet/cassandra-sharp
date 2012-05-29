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

namespace CassandraSharp.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;

    public class CassandraDataParameterCollection : IDataParameterCollection
    {
        private readonly List<CassandraParameter> _params = new List<CassandraParameter>();

        private readonly object _sync = new object();

        public IEnumerator GetEnumerator()
        {
            return _params.GetEnumerator();
        }

        public void CopyTo(Array array, int index)
        {
            CassandraParameter[] prms = _params.ToArray();
            prms.CopyTo(array, index);
        }

        public int Count
        {
            get { return _params.Count; }
        }

        public object SyncRoot
        {
            get { return _sync; }
        }

        public bool IsSynchronized
        {
            get { return false; }
        }

        public int Add(object value)
        {
            CassandraParameter prm = new CassandraParameter {Value = value};
            _params.Add(prm);
            return _params.Count - 1;
        }

        public bool Contains(object value)
        {
            return _params.Any(x => x.Value == value);
        }

        public void Clear()
        {
            _params.Clear();
        }

        public int IndexOf(object value)
        {
            int idx = _params.FindIndex(x => x.Value == value);
            return idx;
        }

        public void Insert(int index, object value)
        {
            CassandraParameter prm = new CassandraParameter {Value = value};
            _params.Insert(index, prm);
        }

        public void Remove(object value)
        {
            _params.RemoveAll(x => x.Value == value);
        }

        public void RemoveAt(int index)
        {
            _params.RemoveAt(index);
        }

        object IList.this[int index]
        {
            get { return _params[index]; }
            set
            {
                CassandraParameter prm = (CassandraParameter) value;
                _params[index] = prm;
            }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool IsFixedSize
        {
            get { return false; }
        }

        public bool Contains(string parameterName)
        {
            return _params.Any(x => x.ParameterName == parameterName);
        }

        public int IndexOf(string parameterName)
        {
            return _params.FindIndex(x => x.ParameterName == parameterName);
        }

        public void RemoveAt(string parameterName)
        {
            int idx = IndexOf(parameterName);
            _params.RemoveAt(idx);
        }

        object IDataParameterCollection.this[string parameterName]
        {
            get { return _params.FirstOrDefault(x => x.ParameterName == parameterName); }
            set
            {
                CassandraParameter prm = (CassandraParameter) value;
                int idx = IndexOf(parameterName);
                _params[idx] = prm;
            }
        }
    }
}