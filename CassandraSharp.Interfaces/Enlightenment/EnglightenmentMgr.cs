// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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

namespace CassandraSharp.Enlightenment
{
    using System;
    using CassandraSharp.Extensibility;

    internal class EnglightenmentMgr
    {
        private static readonly Lazy<IClusterManager> _clusterMgr = new Lazy<IClusterManager>(CreateClusterManager);

        private static readonly Lazy<IFuture> _future = new Lazy<IFuture>(CreateFuture);

        private static readonly Lazy<IPocoCommandFactory> _pocoCommand = new Lazy<IPocoCommandFactory>(CreatePocoCommandFactory);

        private static readonly Lazy<IPropertyBagCommandFactory> _propertyBagCommand = new Lazy<IPropertyBagCommandFactory>(CreatePropertyBagCommandFactory);

        private static readonly Lazy<ICommandFactory> _commandFactory = new Lazy<ICommandFactory>(CreateCommandFactory);

        private static readonly Lazy<IDataMapperFactory> _pocoDataMapperFactory = new Lazy<IDataMapperFactory>(CreatePocoDataMapperFactory);

        private static readonly Lazy<IDataMapperFactory> _propertyBagDataMapperFactory = new Lazy<IDataMapperFactory>(CreatePropertyBagDataMapperFactory);

        private static readonly Lazy<IDataMapperFactory> _ordinalDataMapperFactory = new Lazy<IDataMapperFactory>(CreateOrdinalDataMapperFactory);

        public static IClusterManager ClusterManager()
        {
            return _clusterMgr.Value;
        }

        public static IFuture Future()
        {
            return _future.Value;
        }

        public static IPocoCommandFactory PocoCommandFactory()
        {
            return _pocoCommand.Value;
        }

        public static IPropertyBagCommandFactory PropertyBagCommandFactory()
        {
            return _propertyBagCommand.Value;
        }

        public static ICommandFactory CommandFactory()
        {
            return _commandFactory.Value;
        }

        public static IDataMapperFactory PocoDataMapperFactory()
        {
            return _pocoDataMapperFactory.Value;
        }

        public static IDataMapperFactory PropertyBagDataMapperFactory()
        {
            return _propertyBagDataMapperFactory.Value;
        }

        public static IDataMapperFactory OrdinalDataMapperFactory()
        {
            return _ordinalDataMapperFactory.Value;
        }

        private static T Create<T>(string typeName)
        {
            Type type = Type.GetType(typeName, true);
            T t = (T) Activator.CreateInstance(type);
            return t;
        }

        private static IClusterManager CreateClusterManager()
        {
            const string typeName = "CassandraSharp.Enlightenment.ClusterManager, CassandraSharp";
            return Create<IClusterManager>(typeName);
        }

        private static IFuture CreateFuture()
        {
            const string typeName = "CassandraSharp.Enlightenment.Future, CassandraSharp";
            return Create<IFuture>(typeName);
        }

        private static IPocoCommandFactory CreatePocoCommandFactory()
        {
            const string typeName = "CassandraSharp.Enlightenment.PocoCommandFactory, CassandraSharp";
            return Create<IPocoCommandFactory>(typeName);
        }

        private static IPropertyBagCommandFactory CreatePropertyBagCommandFactory()
        {
            const string typeName = "CassandraSharp.Enlightenment.PropertyBagCommandFactory, CassandraSharp";
            return Create<IPropertyBagCommandFactory>(typeName);
        }

        private static ICommandFactory CreateCommandFactory()
        {
            const string typeName = "CassandraSharp.Enlightenment.CommandFactory, CassandraSharp";
            return Create<ICommandFactory>(typeName);
        }

        private static IDataMapperFactory CreatePocoDataMapperFactory()
        {
            const string typeName = "CassandraSharp.Enlightenment.PocoDataMapperFactory, CassandraSharp";
            return Create<IDataMapperFactory>(typeName);
        }

        private static IDataMapperFactory CreatePropertyBagDataMapperFactory()
        {
            const string typeName = "CassandraSharp.Enlightenment.PropertyBagDataMapperFactory, CassandraSharp";
            return Create<IDataMapperFactory>(typeName);
        }

        private static IDataMapperFactory CreateOrdinalDataMapperFactory()
        {
            const string typeName = "CassandraSharp.Enlightenment.OrdinalDataMapperFactory, CassandraSharp";
            return Create<IDataMapperFactory>(typeName);
        }
    }
}