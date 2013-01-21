/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Thrift;
using Thrift.Collections;
using System.Runtime.Serialization;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Cassandra
{

  /// <summary>
  /// Row returned from a CQL query
  /// </summary>
  #if !SILVERLIGHT
  [Serializable]
  #endif
  public partial class CqlRow : TBase
  {
    private byte[] _key;
    private List<Column> _columns;

    public byte[] Key
    {
      get
      {
        return _key;
      }
      set
      {
        __isset.key = true;
        this._key = value;
      }
    }

    public List<Column> Columns
    {
      get
      {
        return _columns;
      }
      set
      {
        __isset.columns = true;
        this._columns = value;
      }
    }


    public Isset __isset;
    #if !SILVERLIGHT
    [Serializable]
    #endif
    public struct Isset {
      public bool key;
      public bool columns;
    }

    public CqlRow() {
    }

    public void Read (TProtocol iprot)
    {
      TField field;
      iprot.ReadStructBegin();
      while (true)
      {
        field = iprot.ReadFieldBegin();
        if (field.Type == TType.Stop) { 
          break;
        }
        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.String) {
              Key = iprot.ReadBinary();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 2:
            if (field.Type == TType.List) {
              {
                Columns = new List<Column>();
                TList _list69 = iprot.ReadListBegin();
                for( int _i70 = 0; _i70 < _list69.Count; ++_i70)
                {
                  Column _elem71 = new Column();
                  _elem71 = new Column();
                  _elem71.Read(iprot);
                  Columns.Add(_elem71);
                }
                iprot.ReadListEnd();
              }
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          default: 
            TProtocolUtil.Skip(iprot, field.Type);
            break;
        }
        iprot.ReadFieldEnd();
      }
      iprot.ReadStructEnd();
    }

    public void Write(TProtocol oprot) {
      TStruct struc = new TStruct("CqlRow");
      oprot.WriteStructBegin(struc);
      TField field = new TField();
      if (Key != null && __isset.key) {
        field.Name = "key";
        field.Type = TType.String;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        oprot.WriteBinary(Key);
        oprot.WriteFieldEnd();
      }
      if (Columns != null && __isset.columns) {
        field.Name = "columns";
        field.Type = TType.List;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        {
          oprot.WriteListBegin(new TList(TType.Struct, Columns.Count));
          foreach (Column _iter72 in Columns)
          {
            _iter72.Write(oprot);
          }
          oprot.WriteListEnd();
        }
        oprot.WriteFieldEnd();
      }
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }

    public override string ToString() {
      StringBuilder sb = new StringBuilder("CqlRow(");
      sb.Append("Key: ");
      sb.Append(Key);
      sb.Append(",Columns: ");
      sb.Append(Columns);
      sb.Append(")");
      return sb.ToString();
    }

  }

}
