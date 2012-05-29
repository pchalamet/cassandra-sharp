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

namespace CassandraSharp.Utils
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Xml;
    using System.Xml.Serialization;

    public class MiniXmlSerializer
    {
        private readonly Type _type;

        public MiniXmlSerializer(Type type)
        {
            _type = type;
        }

        public object Deserialize(XmlReader xmlReader)
        {
            XmlRootAttribute xmlRootAttribute = (XmlRootAttribute) _type.GetCustomAttributes(typeof(XmlRootAttribute), false).SingleOrDefault();
            string rootElement = null == xmlRootAttribute
                                     ? _type.Name
                                     : xmlRootAttribute.ElementName;

            xmlReader.MoveToContent();

            if (xmlReader.LocalName != rootElement)
            {
                throw new XmlException("Unexpected element");
            }

            object res = DeserializeCheckedElement(xmlReader, _type);

            // skip end tag
            while (xmlReader.Read())
            {
                xmlReader.MoveToContent();
            }

            if (! xmlReader.EOF)
            {
                throw new XmlException("Invalid xml content");
            }

            return res;
        }

        public static object DeserializeCheckedElement(XmlReader xmlReader, Type type)
        {
            if (!IsScalarType(type))
            {
                ConstructorInfo ctorInfo = type.GetConstructor(new Type[0]);
                object target = ctorInfo.Invoke(new object[0]);
                FeedTargetObject(xmlReader, target);
                return target;
            }

            if (xmlReader.IsEmptyElement)
            {
                return null;
            }

            xmlReader.Read();
            string value = xmlReader.Value;
            xmlReader.Read();
            return GetTargetValue(value, type);
        }

        private static void FeedTargetObject(XmlReader xmlReader, object target)
        {
            FeedTargetObjectAttributes(xmlReader, target);
            FeedTargetObjectElement(xmlReader, target);
        }

        private static void FeedTargetObjectElement(XmlReader xmlReader, object target)
        {
            if (!xmlReader.IsEmptyElement)
            {
                var eltName2MemberInfos = (from mi in target.GetType().GetMembers()
                                           let xmlElt = (XmlElementAttribute) mi.GetCustomAttributes(typeof(XmlElementAttribute), false).SingleOrDefault()
                                           where null != xmlElt
                                           select new {xmlElt.ElementName, MemberInfo = mi}).ToDictionary(x => x.ElementName);

                while (xmlReader.Read() && XmlNodeType.EndElement != xmlReader.NodeType)
                {
                    if (xmlReader.NodeType == XmlNodeType.Element)
                    {
                        string name = xmlReader.Name;

                        MemberInfo mi = eltName2MemberInfos[name].MemberInfo;
                        SetElement(target, mi, xmlReader);
                    }
                }
            }
        }

        private static void FeedTargetObjectAttributes(XmlReader xmlReader, object target)
        {
            if (xmlReader.HasAttributes)
            {
                var attrName2MemberInfos = (from mi in target.GetType().GetMembers()
                                            let xmlAttr = (XmlAttributeAttribute)
                                                          mi.GetCustomAttributes(typeof(XmlAttributeAttribute), false).SingleOrDefault()
                                            where null != xmlAttr
                                            select new {xmlAttr.AttributeName, MemberInfo = mi}).ToDictionary(x => x.AttributeName);

                bool hasMore = xmlReader.MoveToFirstAttribute();
                while (hasMore)
                {
                    string name = xmlReader.Name;
                    string value = xmlReader.Value;

                    MemberInfo mi = attrName2MemberInfos[name].MemberInfo;
                    SetAttribute(target, mi, value);

                    hasMore = xmlReader.MoveToNextAttribute();
                }
                xmlReader.MoveToContent();
            }
        }

        private static void SetAttribute(object target, MemberInfo mi, string value)
        {
            if (mi.MemberType == MemberTypes.Property)
            {
                SetProperty(target, (PropertyInfo) mi, value);
            }
            else
            {
                SetField(target, (FieldInfo) mi, value);
            }
        }

        private static void SetField(object target, FieldInfo fi, string value)
        {
            Type type = fi.FieldType;
            object targetValue = GetTargetValue(value, type);
            fi.SetValue(target, targetValue);
        }

        private static object GetTargetValue(string value, Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                type = type.GetGenericArguments()[0];
            }

            // enum
            if (type.IsEnum)
            {
                return Enum.Parse(type, value);
            }

            // string is not in XmlConvert
            if (type == typeof(string))
            {
                return value;
            }

            // DateTime is depreacated in XmlConvert
            if (type == typeof(DateTime))
            {
                return XmlConvert.ToDateTime(value, XmlDateTimeSerializationMode.Utc);
            }

            // use XmlConvert for all other simple types
            string methodName = "To" + type.Name;
            MethodInfo miTo = typeof(XmlConvert).GetMethod(methodName);
            if (null == miTo)
            {
                return null;
            }

            object targetValue = miTo.Invoke(null, new object[] {value});
            return targetValue;
        }

        private static void SetProperty(object target, PropertyInfo pi, string value)
        {
            Type type = pi.PropertyType;
            object targetValue = GetTargetValue(value, type);
            pi.SetValue(target, targetValue, null);
        }

        private static bool IsScalarType(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                type = type.GetGenericArguments()[0];
            }

            if (type.IsEnum)
            {
                return true;
            }

            if (type == typeof(string))
            {
                return true;
            }

            if (type == typeof(DateTime))
            {
                return true;
            }

            string methodName = "To" + type.Name;
            MethodInfo miTo = typeof(XmlConvert).GetMethod(methodName);
            if (null != miTo)
            {
                return true;
            }

            return false;
        }

        private static void SetElement(object target, MemberInfo mi, XmlReader xmlReader)
        {
            Type miType;
            PropertyInfo pi = null;
            FieldInfo fi = null;
            bool isScalar;
            if (mi.MemberType == MemberTypes.Property)
            {
                pi = (PropertyInfo) mi;
                miType = pi.PropertyType;
                isScalar = IsScalarType(pi.PropertyType);
            }
            else
            {
                fi = (FieldInfo) mi;
                miType = fi.FieldType;
                isScalar = IsScalarType(fi.FieldType);
            }

            if (isScalar)
            {
                string value = xmlReader.Value;
                SetAttribute(target, mi, value);
            }
            else
            {
                Type deserializeType = miType;
                if (miType.IsArray)
                {
                    deserializeType = miType.GetElementType();
                }

                object value = DeserializeCheckedElement(xmlReader, deserializeType);

                if (miType.IsArray)
                {
                    Array existingArray;
                    if (null != pi)
                    {
                        existingArray = (Array) pi.GetValue(target, null);
                    }
                    else
                    {
                        existingArray = (Array) fi.GetValue(target);
                    }

                    if (null != existingArray)
                    {
                        Array biggerArray = Array.CreateInstance(deserializeType, existingArray.Length + 1);
                        Array.Copy(existingArray, biggerArray, existingArray.Length);
                        biggerArray.SetValue(value, existingArray.Length);
                        value = biggerArray;
                    }
                    else
                    {
                        Array newArray = Array.CreateInstance(deserializeType, 1);
                        newArray.SetValue(value, 0);
                        value = newArray;
                    }
                }

                if (null != pi)
                {
                    pi.SetValue(target, value, null);
                }
                else
                {
                    fi.SetValue(target, value);
                }
            }
        }
    }
}