/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.thrift2;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.thrift2.generated.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class ThriftUtilities {

  private ThriftUtilities() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

/*
  */
/**
   * This utility method creates a new Thrift TableDescriptor "struct" based on
   * a HBase HTableDescriptor object.
   *
   * @param in HBase HTableDescriptor object
   * @return Thrift TableDescriptor
   *//*

  public static TTableDescriptor tableDescFromHbase(HTableDescriptor in) {
    TTableDescriptor out = new TTableDescriptor(in.getName(), in.isDeferredLogFlush(),
            in.getMaxFileSize(), in.getMemStoreFlushSize(), null);

    for (HColumnDescriptor columnDescriptor : in.getFamilies()) {
      out.addToFamilies(colDescFromHbase(columnDescriptor));
    }
    return out;
  }

  public static HTableDescriptor tableDescFromThrift(TTableDescriptor in) {
    HTableDescriptor out = new HTableDescriptor(in.getName());
    out.setDeferredLogFlush(in.isDeferredLogFlush());
    out.setMaxFileSize(in.getMaxFileSize());
    out.setMemStoreFlushSize(in.getMemStoreFlushSize());
    out.setReadOnly(in.isReadOnly());

    for (TColumnFamilyDescriptor colDesc : in.getFamilies()) {
      out.addFamily(colDescFromThrift(colDesc));
    }

    return out;
  }

  */
/**
   * This utility method creates a new Thrift ColumnDescriptor "struct" based on
   * an Hbase HColumnDescriptor object.
   *
   * @param in HBase HColumnDescriptor object
   * @return Thrift ColumnDescriptor
   *//*

  public static TColumnFamilyDescriptor colDescFromHbase(HColumnDescriptor in) {
    return new TColumnFamilyDescriptor(in.getName(),
            TCompressionAlgorithm.valueOf(in.getCompression().toString()), in.getMaxVersions(),
            in.getBlocksize(), in.isInMemory(), in.getTimeToLive(), in.isBlockCacheEnabled(),
            in.isBloomfilter());
  }

  */
/**
   * This utility method creates a new Hbase HColumnDescriptor object based on a
   * Thrift ColumnDescriptor "struct".
   *
   * @param in Thrift ColumnDescriptor object
   * @return HColumnDescriptor
   * @throws IllegalArgument
   *//*

  public static HColumnDescriptor colDescFromThrift(TColumnFamilyDescriptor in) {
    HColumnDescriptor out = new HColumnDescriptor(in.getName());
    out.setMaxVersions(in.getMaxVersions());
    out.setCompressionType(Compression.Algorithm.valueOf(in.getCompression().toString()));
    out.setBlockCacheEnabled(in.isBlockCacheEnabled());
    out.setBlocksize(in.getBlocksize());
    out.setBloomfilter(in.isBloomfilterEnabled());
    out.setInMemory(in.isInMemory());
    int ttl = (in.getTimeToLive() != -1) ? in.getTimeToLive() : HConstants.FOREVER;
    out.setTimeToLive(ttl);
    return out;
  }
*/

  /**
   * Creates a {@link Get} (HBase) from a {@link TGet} (Thrift).
   *
   * This ignores any timestamps set on {@link TColumn} objects.
   *
   * @param in the <code>TGet</code> to convert
   *
   * @return <code>Get</code> object
   *
   * @throws IOException if an invalid time range or max version parameter is given
   */
  public static Get getFromThrift(TGet in) throws IOException {
    Get out = new Get(in.getRow());

    // Timestamp overwrites time range if both are set
    if (in.isSetTimestamp()) {
      out.setTimeStamp(in.getTimestamp());
    } else if (in.isSetTimeRange()) {
      out.setTimeRange(in.getTimeRange().getMinStamp(), in.getTimeRange().getMaxStamp());
    }

    if (in.isSetMaxVersions()) {
      out.setMaxVersions(in.getMaxVersions());
    }

    if (!in.isSetColumns()) {
      return out;
    }

    for (TColumn column : in.getColumns()) {
      if (column.isSetQualifier()) {
        out.addColumn(column.getFamily(), column.getQualifier());
      } else {
        out.addFamily(column.getFamily());
      }
    }

    return out;
  }

  /**
   * Converts multiple {@link TGet}s (Thrift) into a list of {@link Get}s (HBase).
   *
   * @param in list of <code>TGet</code>s to convert
   *
   * @return list of <code>Get</code> objects
   *
   * @throws IOException if an invalid time range or max version parameter is given
   * @see #getFromThrift(TGet)
   */
  public static List<Get> getsFromThrift(List<TGet> in) throws IOException {
    List<Get> out = new ArrayList<Get>(in.size());
    for (TGet get : in) {
      out.add(getFromThrift(get));
    }
    return out;
  }

  /**
   * Creates a {@link TResult} (Thrift) from a {@link Result} (HBase).
   *
   * @param in the <code>Result</code> to convert
   *
   * @return converted result, returns an empty result if the input is <code>null</code>
   */
  public static TResult resultFromHBase(Result in) {
    TResult out = new TResult();

    out.setRow(in.getRow());
    List<TColumnValue> values = new ArrayList<TColumnValue>();

    // Map<family, Map<qualifier, Map<timestamp, value>>>
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : in.getMap().entrySet()) {
      for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifier : family.getValue().entrySet()) {
        for (Map.Entry<Long, byte[]> timestamp : qualifier.getValue().entrySet()) {
          TColumnValue col = new TColumnValue();
          col.setFamily(family.getKey());
          col.setQualifier(qualifier.getKey());
          col.setTimestamp(timestamp.getKey());
          values.add(col);
        }
      }
    }

    out.setEntries(values);
    return out;
  }

  /**
   * Converts multiple {@link Result}s (HBase) into a list of {@link TResult}s (Thrift).
   *
   * @param in array of <code>Result</code>s to convert
   *
   * @return list of converted <code>TResult</code>s
   *
   * @see #resultFromHBase(Result)
   */
  public static List<TResult> resultsFromHBase(Result[] in) {
    List<TResult> out = new ArrayList<TResult>(in.length);
    for (Result result : in) {
      out.add(resultFromHBase(result));
    }
    return out;
  }

  /**
   * Creates a {@link Put} (HBase) from a {@link TPut} (Thrift)
   *
   * @param in the <code>TPut</code> to convert
   *
   * @return converted <code>Put</code>
   */
  public static Put putFromThrift(TPut in) {
    Put out;

    if (in.isSetTimestamp()) {
      out = new Put(in.getRow(), in.getTimestamp(), null);
    } else {
      out = new Put(in.getRow());
    }

    out.setWriteToWAL(in.isWriteToWal());

    for (TColumnValue columnValue : in.getColumnValues()) {
      if (columnValue.isSetTimestamp()) {
        out.add(columnValue.getFamily(), columnValue.getQualifier(), columnValue.getTimestamp(),
          columnValue.getValue());
      } else {
        out.add(columnValue.getFamily(), columnValue.getQualifier(), columnValue.getValue());
      }
    }

    return out;
  }

  /**
   * Converts multiple {@link TPut}s (Thrift) into a list of {@link Put}s (HBase).
   *
   * @param in list of <code>TPut</code>s to convert
   *
   * @return list of converted <code>Put</code>s
   *
   * @see #putFromThrift(TPut)
   */
  public static List<Put> putsFromThrift(List<TPut> in) {
    List<Put> out = new ArrayList<Put>(in.size());
    for (TPut put : in) {
      out.add(putFromThrift(put));
    }
    return out;
  }

  /**
   * Creates a {@link Delete} (HBase) from a {@link TDelete} (Thrift).
   *
   * @param in the <code>TDelete</code> to convert
   *
   * @return converted <code>Delete</code>
   */
  public static Delete deleteFromThrift(TDelete in) {
    Delete out;

    if (in.isSetColumns()) {
      out = new Delete(in.getRow());
      for (TColumn column : in.getColumns()) {
        if (column.isSetQualifier()) {
          if (column.isSetTimestamp()) {
            out.deleteColumn(column.getFamily(), column.getQualifier(), column.getTimestamp());
          } else {
            out.deleteColumn(column.getFamily(), column.getQualifier());
          }

        } else {
          if (column.isSetTimestamp()) {
            out.deleteFamily(column.getFamily(), column.getTimestamp());
          } else {
            out.deleteFamily(column.getFamily());
          }
        }
      }
    } else {
      if (in.isSetTimestamp()) {
        out = new Delete(in.getRow(), in.getTimestamp(), null);
      } else {
        out = new Delete(in.getRow());
      }
    }

    return out;
  }

  /**
   * Converts multiple {@link TDelete}s (Thrift) into a list of {@link Delete}s (HBase).
   *
   * @param in list of <code>TDelete</code>s to convert
   *
   * @return list of converted <code>Delete</code>s
   *
   * @see #deleteFromThrift(TDelete)
   */

  public static List<Delete> deletesFromThrift(List<TDelete> in) {
    List<Delete> out = new ArrayList<Delete>(in.size());
    for (TDelete delete : in) {
      out.add(deleteFromThrift(delete));
    }
    return out;
  }

  // TODO: Not yet entirely sure what the best way to do this is
  public static TDelete deleteFromHBase(Delete in) {
    TDelete out = new TDelete(ByteBuffer.wrap(in.getRow()));

    List<TColumn> columns = new ArrayList<TColumn>();

    // Map<family, List<KeyValue>>
    for (Map.Entry<byte[], List<KeyValue>> listEntry : in.getFamilyMap().entrySet()) {
      TColumn column = new TColumn(ByteBuffer.wrap(listEntry.getKey()));
      for (KeyValue keyValue : listEntry.getValue()) {
        if (keyValue.isDeleteFamily() && keyValue.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
          column.setTimestamp(keyValue.getTimestamp());
        }
      }
    }

    return out;
  }

  public static List<TDelete> deletesFromHBase(List<Delete> in) {
    List<TDelete> out = new ArrayList<TDelete>(in.size());
    for (Delete delete : in) {
      if (delete == null) {
        out.add(null);
      } else {
        out.add(deleteFromHBase(delete));
      }
    }
    return out;
  }

  public static Scan scanFromThrift(TScan in) {
    Scan out = new Scan();

    if (in.isSetStartRow()) out.setStartRow(in.getStartRow());
    if (in.isSetStopRow()) out.setStopRow(in.getStopRow());
    if (in.isSetCaching()) out.setCaching(in.getCaching());

    // TODO: Timestamps
    if (in.isSetColumns()) {
      for (TColumn column : in.getColumns()) {
        if (column.isSetQualifier()) {
          out.addColumn(column.getFamily(), column.getQualifier());
        } else {
          out.addFamily(column.getFamily());
        }
      }
    }

    return out;
  }

}
