/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.threeglav.sh.bauk.rpc;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputFeed implements org.apache.thrift.TBase<InputFeed, InputFeed._Fields>, java.io.Serializable, Cloneable, Comparable<InputFeed> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("InputFeed");

  private static final org.apache.thrift.protocol.TField FEED_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("feedName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField LAST_MODIFIED_TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("lastModifiedTimestamp", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField SIZE_BYTES_FIELD_DESC = new org.apache.thrift.protocol.TField("sizeBytes", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField DATA_FIELD_DESC = new org.apache.thrift.protocol.TField("data", org.apache.thrift.protocol.TType.LIST, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new InputFeedStandardSchemeFactory());
    schemes.put(TupleScheme.class, new InputFeedTupleSchemeFactory());
  }

  public String feedName; // required
  public long lastModifiedTimestamp; // required
  public long sizeBytes; // required
  public List<String> data; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FEED_NAME((short)1, "feedName"),
    LAST_MODIFIED_TIMESTAMP((short)2, "lastModifiedTimestamp"),
    SIZE_BYTES((short)3, "sizeBytes"),
    DATA((short)4, "data");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // FEED_NAME
          return FEED_NAME;
        case 2: // LAST_MODIFIED_TIMESTAMP
          return LAST_MODIFIED_TIMESTAMP;
        case 3: // SIZE_BYTES
          return SIZE_BYTES;
        case 4: // DATA
          return DATA;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __LASTMODIFIEDTIMESTAMP_ISSET_ID = 0;
  private static final int __SIZEBYTES_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FEED_NAME, new org.apache.thrift.meta_data.FieldMetaData("feedName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LAST_MODIFIED_TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("lastModifiedTimestamp", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.SIZE_BYTES, new org.apache.thrift.meta_data.FieldMetaData("sizeBytes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.DATA, new org.apache.thrift.meta_data.FieldMetaData("data", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(InputFeed.class, metaDataMap);
  }

  public InputFeed() {
  }

  public InputFeed(
    String feedName,
    long lastModifiedTimestamp,
    long sizeBytes,
    List<String> data)
  {
    this();
    this.feedName = feedName;
    this.lastModifiedTimestamp = lastModifiedTimestamp;
    setLastModifiedTimestampIsSet(true);
    this.sizeBytes = sizeBytes;
    setSizeBytesIsSet(true);
    this.data = data;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public InputFeed(InputFeed other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetFeedName()) {
      this.feedName = other.feedName;
    }
    this.lastModifiedTimestamp = other.lastModifiedTimestamp;
    this.sizeBytes = other.sizeBytes;
    if (other.isSetData()) {
      List<String> __this__data = new ArrayList<String>(other.data);
      this.data = __this__data;
    }
  }

  public InputFeed deepCopy() {
    return new InputFeed(this);
  }

  @Override
  public void clear() {
    this.feedName = null;
    setLastModifiedTimestampIsSet(false);
    this.lastModifiedTimestamp = 0;
    setSizeBytesIsSet(false);
    this.sizeBytes = 0;
    this.data = null;
  }

  public String getFeedName() {
    return this.feedName;
  }

  public InputFeed setFeedName(String feedName) {
    this.feedName = feedName;
    return this;
  }

  public void unsetFeedName() {
    this.feedName = null;
  }

  /** Returns true if field feedName is set (has been assigned a value) and false otherwise */
  public boolean isSetFeedName() {
    return this.feedName != null;
  }

  public void setFeedNameIsSet(boolean value) {
    if (!value) {
      this.feedName = null;
    }
  }

  public long getLastModifiedTimestamp() {
    return this.lastModifiedTimestamp;
  }

  public InputFeed setLastModifiedTimestamp(long lastModifiedTimestamp) {
    this.lastModifiedTimestamp = lastModifiedTimestamp;
    setLastModifiedTimestampIsSet(true);
    return this;
  }

  public void unsetLastModifiedTimestamp() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __LASTMODIFIEDTIMESTAMP_ISSET_ID);
  }

  /** Returns true if field lastModifiedTimestamp is set (has been assigned a value) and false otherwise */
  public boolean isSetLastModifiedTimestamp() {
    return EncodingUtils.testBit(__isset_bitfield, __LASTMODIFIEDTIMESTAMP_ISSET_ID);
  }

  public void setLastModifiedTimestampIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __LASTMODIFIEDTIMESTAMP_ISSET_ID, value);
  }

  public long getSizeBytes() {
    return this.sizeBytes;
  }

  public InputFeed setSizeBytes(long sizeBytes) {
    this.sizeBytes = sizeBytes;
    setSizeBytesIsSet(true);
    return this;
  }

  public void unsetSizeBytes() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __SIZEBYTES_ISSET_ID);
  }

  /** Returns true if field sizeBytes is set (has been assigned a value) and false otherwise */
  public boolean isSetSizeBytes() {
    return EncodingUtils.testBit(__isset_bitfield, __SIZEBYTES_ISSET_ID);
  }

  public void setSizeBytesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __SIZEBYTES_ISSET_ID, value);
  }

  public int getDataSize() {
    return (this.data == null) ? 0 : this.data.size();
  }

  public java.util.Iterator<String> getDataIterator() {
    return (this.data == null) ? null : this.data.iterator();
  }

  public void addToData(String elem) {
    if (this.data == null) {
      this.data = new ArrayList<String>();
    }
    this.data.add(elem);
  }

  public List<String> getData() {
    return this.data;
  }

  public InputFeed setData(List<String> data) {
    this.data = data;
    return this;
  }

  public void unsetData() {
    this.data = null;
  }

  /** Returns true if field data is set (has been assigned a value) and false otherwise */
  public boolean isSetData() {
    return this.data != null;
  }

  public void setDataIsSet(boolean value) {
    if (!value) {
      this.data = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FEED_NAME:
      if (value == null) {
        unsetFeedName();
      } else {
        setFeedName((String)value);
      }
      break;

    case LAST_MODIFIED_TIMESTAMP:
      if (value == null) {
        unsetLastModifiedTimestamp();
      } else {
        setLastModifiedTimestamp((Long)value);
      }
      break;

    case SIZE_BYTES:
      if (value == null) {
        unsetSizeBytes();
      } else {
        setSizeBytes((Long)value);
      }
      break;

    case DATA:
      if (value == null) {
        unsetData();
      } else {
        setData((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FEED_NAME:
      return getFeedName();

    case LAST_MODIFIED_TIMESTAMP:
      return Long.valueOf(getLastModifiedTimestamp());

    case SIZE_BYTES:
      return Long.valueOf(getSizeBytes());

    case DATA:
      return getData();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FEED_NAME:
      return isSetFeedName();
    case LAST_MODIFIED_TIMESTAMP:
      return isSetLastModifiedTimestamp();
    case SIZE_BYTES:
      return isSetSizeBytes();
    case DATA:
      return isSetData();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof InputFeed)
      return this.equals((InputFeed)that);
    return false;
  }

  public boolean equals(InputFeed that) {
    if (that == null)
      return false;

    boolean this_present_feedName = true && this.isSetFeedName();
    boolean that_present_feedName = true && that.isSetFeedName();
    if (this_present_feedName || that_present_feedName) {
      if (!(this_present_feedName && that_present_feedName))
        return false;
      if (!this.feedName.equals(that.feedName))
        return false;
    }

    boolean this_present_lastModifiedTimestamp = true;
    boolean that_present_lastModifiedTimestamp = true;
    if (this_present_lastModifiedTimestamp || that_present_lastModifiedTimestamp) {
      if (!(this_present_lastModifiedTimestamp && that_present_lastModifiedTimestamp))
        return false;
      if (this.lastModifiedTimestamp != that.lastModifiedTimestamp)
        return false;
    }

    boolean this_present_sizeBytes = true;
    boolean that_present_sizeBytes = true;
    if (this_present_sizeBytes || that_present_sizeBytes) {
      if (!(this_present_sizeBytes && that_present_sizeBytes))
        return false;
      if (this.sizeBytes != that.sizeBytes)
        return false;
    }

    boolean this_present_data = true && this.isSetData();
    boolean that_present_data = true && that.isSetData();
    if (this_present_data || that_present_data) {
      if (!(this_present_data && that_present_data))
        return false;
      if (!this.data.equals(that.data))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(InputFeed other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetFeedName()).compareTo(other.isSetFeedName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFeedName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.feedName, other.feedName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLastModifiedTimestamp()).compareTo(other.isSetLastModifiedTimestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLastModifiedTimestamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lastModifiedTimestamp, other.lastModifiedTimestamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSizeBytes()).compareTo(other.isSetSizeBytes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSizeBytes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sizeBytes, other.sizeBytes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetData()).compareTo(other.isSetData());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetData()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.data, other.data);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InputFeed(");
    boolean first = true;

    sb.append("feedName:");
    if (this.feedName == null) {
      sb.append("null");
    } else {
      sb.append(this.feedName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("lastModifiedTimestamp:");
    sb.append(this.lastModifiedTimestamp);
    first = false;
    if (!first) sb.append(", ");
    sb.append("sizeBytes:");
    sb.append(this.sizeBytes);
    first = false;
    if (!first) sb.append(", ");
    sb.append("data:");
    if (this.data == null) {
      sb.append("null");
    } else {
      sb.append(this.data);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class InputFeedStandardSchemeFactory implements SchemeFactory {
    public InputFeedStandardScheme getScheme() {
      return new InputFeedStandardScheme();
    }
  }

  private static class InputFeedStandardScheme extends StandardScheme<InputFeed> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, InputFeed struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FEED_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.feedName = iprot.readString();
              struct.setFeedNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LAST_MODIFIED_TIMESTAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.lastModifiedTimestamp = iprot.readI64();
              struct.setLastModifiedTimestampIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SIZE_BYTES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.sizeBytes = iprot.readI64();
              struct.setSizeBytesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DATA
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.data = new ArrayList<String>(_list0.size);
                for (int _i1 = 0; _i1 < _list0.size; ++_i1)
                {
                  String _elem2;
                  _elem2 = iprot.readString();
                  struct.data.add(_elem2);
                }
                iprot.readListEnd();
              }
              struct.setDataIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, InputFeed struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.feedName != null) {
        oprot.writeFieldBegin(FEED_NAME_FIELD_DESC);
        oprot.writeString(struct.feedName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(LAST_MODIFIED_TIMESTAMP_FIELD_DESC);
      oprot.writeI64(struct.lastModifiedTimestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SIZE_BYTES_FIELD_DESC);
      oprot.writeI64(struct.sizeBytes);
      oprot.writeFieldEnd();
      if (struct.data != null) {
        oprot.writeFieldBegin(DATA_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.data.size()));
          for (String _iter3 : struct.data)
          {
            oprot.writeString(_iter3);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class InputFeedTupleSchemeFactory implements SchemeFactory {
    public InputFeedTupleScheme getScheme() {
      return new InputFeedTupleScheme();
    }
  }

  private static class InputFeedTupleScheme extends TupleScheme<InputFeed> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, InputFeed struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetFeedName()) {
        optionals.set(0);
      }
      if (struct.isSetLastModifiedTimestamp()) {
        optionals.set(1);
      }
      if (struct.isSetSizeBytes()) {
        optionals.set(2);
      }
      if (struct.isSetData()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetFeedName()) {
        oprot.writeString(struct.feedName);
      }
      if (struct.isSetLastModifiedTimestamp()) {
        oprot.writeI64(struct.lastModifiedTimestamp);
      }
      if (struct.isSetSizeBytes()) {
        oprot.writeI64(struct.sizeBytes);
      }
      if (struct.isSetData()) {
        {
          oprot.writeI32(struct.data.size());
          for (String _iter4 : struct.data)
          {
            oprot.writeString(_iter4);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, InputFeed struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.feedName = iprot.readString();
        struct.setFeedNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.lastModifiedTimestamp = iprot.readI64();
        struct.setLastModifiedTimestampIsSet(true);
      }
      if (incoming.get(2)) {
        struct.sizeBytes = iprot.readI64();
        struct.setSizeBytesIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list5 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.data = new ArrayList<String>(_list5.size);
          for (int _i6 = 0; _i6 < _list5.size; ++_i6)
          {
            String _elem7;
            _elem7 = iprot.readString();
            struct.data.add(_elem7);
          }
        }
        struct.setDataIsSet(true);
      }
    }
  }

}

