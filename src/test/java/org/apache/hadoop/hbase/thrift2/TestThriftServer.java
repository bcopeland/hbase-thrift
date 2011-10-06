/*
 * Copyright 2009 The Apache Software Foundation
 *
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.compiler.ir.operands.Array;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Equivalence;

/**
 * Unit testing for ThriftServer.HBaseHandler, a part of the
 * org.apache.hadoop.hbase.thrift2 package.
 */
public class TestThriftServer {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  // Static names for tables, columns, rows, and values
  private static byte[] tableAname = Bytes.toBytes("tableA");
  private static byte[] tableBname = Bytes.toBytes("tableB");
  private static byte[] familyAname = Bytes.toBytes("familyA");
  private static byte[] familyBname = Bytes.toBytes("familyB");
  private static byte[] qualifierAname = Bytes.toBytes("qualifierA");
  private static byte[] qualifierBname = Bytes.toBytes("qualifierB");
  private static byte[] valueAname = Bytes.toBytes("valueA");
  private static byte[] valueBname = Bytes.toBytes("valueB");
  private static byte[] valueCname = Bytes.toBytes("valueC");
  private static byte[] valueDname = Bytes.toBytes("valueD");
  private static HColumnDescriptor[] families = new HColumnDescriptor[] {
      new HColumnDescriptor(familyAname),
      new HColumnDescriptor(familyBname, 2, HColumnDescriptor.DEFAULT_COMPRESSION, HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE, HColumnDescriptor.DEFAULT_TTL, HColumnDescriptor.DEFAULT_BLOOMFILTER)};

  public void assertTColumnValuesEqual(List<TColumnValue> columnValuesA, List<TColumnValue> columnValuesB) {
    assertEquals(columnValuesA.size(), columnValuesB.size());
    Comparator<TColumnValue> comparator = new Comparator<TColumnValue>() {
      @Override
      public int compare(TColumnValue o1, TColumnValue o2) {
        return Bytes.compareTo(Bytes.add(o1.getFamily(), o1.getQualifier()), Bytes.add(o2.getFamily(), o2.getQualifier()));
      }
    };
    Collections.sort(columnValuesA, comparator);
    Collections.sort(columnValuesB, comparator);

    for (int i = 0; i < columnValuesA.size(); i++) {
      TColumnValue a = columnValuesA.get(i);
      TColumnValue b = columnValuesB.get(i);
      assertArrayEquals(a.getFamily(), b.getFamily());
      assertArrayEquals(a.getQualifier(), b.getQualifier());
      assertArrayEquals(a.getValue(), b.getValue());
    }
  }

  /**
   * Runs all of the tests under a single JUnit test method. We
   * consolidate all testing to one method because HBaseClusterTestCase
   * is prone to OutOfMemoryExceptions when there are three or more
   * JUnit test methods.
   * 
   * @throws Exception
   *           public void testAll() throws Exception {
   *           // Run all tests
   *           doTestTableCreateDrop();
   *           doTestTableMutations();
   *           doTestTableTimestampsAndColumns();
   *           doTestTableScanners();
   *           }
   */

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    HBaseAdmin admin = new HBaseAdmin(new Configuration());
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableAname);
    for (HColumnDescriptor family : families) {
      tableDescriptor.addFamily(family);
    }
    admin.createTable(tableDescriptor);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {

  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables. Also
   * tests that creating a table with an invalid column name yields an
   * IllegalArgument exception.
   * 
   * @throws Exception
   * @Test
   *       public void doTestTableCreateDrop() throws Exception {
   *       ThriftHBaseServiceHandler handler = new ThriftHBaseServiceHandler();
   *       // Create/enable/disable/delete tables, ensure methods act correctly
   *       /*
   *       assertTrue(handler.isMasterRunning());
   *       assertEquals(handler.getTableNames().size(), 0);
   *       handler.createTable(tableAname, getColumnDescriptors());
   *       assertEquals(handler.getTableNames().size(), 1);
   *       assertEquals(handler.getColumnDescriptors(tableAname).size(), 2);
   *       assertTrue(handler.isTableEnabled(tableAname));
   *       handler.createTable(tableBname, new ArrayList<ColumnDescriptor>());
   *       assertEquals(handler.getTableNames().size(), 2);
   *       handler.disableTable(tableBname);
   *       assertFalse(handler.isTableEnabled(tableBname));
   *       handler.deleteTable(tableBname);
   *       assertEquals(handler.getTableNames().size(), 1);
   *       handler.disableTable(tableAname);
   *       assertFalse(handler.isTableEnabled(tableAname));
   *       handler.enableTable(tableAname);
   *       assertTrue(handler.isTableEnabled(tableAname));
   *       handler.disableTable(tableAname);
   *       handler.deleteTable(tableAname);
   *       }
   *       /**
   *       Tests adding a series of Mutations and BatchMutations, including a
   *       delete mutation. Also tests data retrieval, and getting back multiple
   *       versions.
   * @throws Exception
   */

  @Test
  public void testPutGet() throws Exception {
    ThriftHBaseServiceHandler handler = new ThriftHBaseServiceHandler();
    byte[] rowName = "testPutGet".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(valueAname)));
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname), ByteBuffer.wrap(valueBname)));
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);

    TGet get = new TGet(ByteBuffer.wrap(rowName));

    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    assertTColumnValuesEqual(columnValues, returnedColumnValues);
  }

  @Test
  public void testDelete() throws Exception {
    ThriftHBaseServiceHandler handler = new ThriftHBaseServiceHandler();
    byte[] rowName = "testDelete".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(valueAname));
    TColumnValue columnValueB = new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname), ByteBuffer.wrap(valueBname));
    columnValues.add(columnValueA);
    columnValues.add(columnValueB);
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);

    TDelete delete = new TDelete(ByteBuffer.wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
    TColumn deleteColumn = new TColumn(ByteBuffer.wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);

    handler.deleteSingle(table, delete);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    List<TColumnValue> expectedColumnValues = new ArrayList<TColumnValue>();
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  @Test
  public void testIncrement() throws Exception {
    ThriftHBaseServiceHandler handler = new ThriftHBaseServiceHandler();
    byte[] rowName = "testIncrement".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(Bytes.toBytes(1L))));
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    handler.put(table, put);

    List<TColumnIncrement> incrementColumns = new ArrayList<TColumnIncrement>();
    incrementColumns.add(new TColumnIncrement(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname)));
    TIncrement increment = new TIncrement(ByteBuffer.wrap(rowName), incrementColumns);
    handler.increment(table, increment);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    TResult result = handler.get(table, get);

    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    TColumnValue columnValue = result.getColumnValues().get(0);
    assertArrayEquals(Bytes.toBytes(2L), columnValue.getValue());
  }

  /**
   * check that checkAndPut fails if the cell does not exist, then put in the cell, then check that the checkAndPut
   * succeeds.
   * 
   * @throws Exception
   */
  @Test
  public void testCheckAndPut() throws Exception {
    ThriftHBaseServiceHandler handler = new ThriftHBaseServiceHandler();
    byte[] rowName = "testCheckAndPut".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(ByteBuffer.wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<TColumnValue>();
    TColumnValue columnValueB = new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname), ByteBuffer.wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(ByteBuffer.wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    assertFalse(handler.checkAndPut(table, ByteBuffer.wrap(rowName), ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname), putB));

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    TResult result = handler.get(table, get);
    assertEquals(0, result.getColumnValuesSize());

    handler.put(table, putA);

    assertTrue(handler.checkAndPut(table, ByteBuffer.wrap(rowName), ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname), putB));
    
    result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    List<TColumnValue> expectedColumnValues = new ArrayList<TColumnValue>();
    expectedColumnValues.add(columnValueA);
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }
  /**
   * // Apply a few Mutations to rowA
   * // mutations.add(new Mutation(false, columnAname, valueAname));
   * // mutations.add(new Mutation(false, columnBname, valueBname));
   * handler.mutateRow(tableAname, rowAname, getMutations());
   * // Assert that the changes were made
   * assertTrue(Bytes.equals(valueAname, handler.get(tableAname, rowAname, columnAname).get(0).value));
   * TRowResult rowResult1 = handler.getRow(tableAname, rowAname).get(0);
   * assertTrue(Bytes.equals(rowAname, rowResult1.row));
   * assertTrue(Bytes.equals(valueBname, rowResult1.columns.get(columnBname).value));
   * // Apply a few BatchMutations for rowA and rowB
   * // rowAmutations.add(new Mutation(true, columnAname, null));
   * // rowAmutations.add(new Mutation(false, columnBname, valueCname));
   * // batchMutations.add(new BatchMutation(rowAname, rowAmutations));
   * // Mutations to rowB
   * // rowBmutations.add(new Mutation(false, columnAname, valueCname));
   * // rowBmutations.add(new Mutation(false, columnBname, valueDname));
   * // batchMutations.add(new BatchMutation(rowBname, rowBmutations));
   * handler.mutateRows(tableAname, getBatchMutations());
   * // Assert that changes were made to rowA
   * List<TCell> cells = handler.get(tableAname, rowAname, columnAname);
   * assertFalse(cells.size() > 0);
   * assertTrue(Bytes.equals(valueCname, handler.get(tableAname, rowAname, columnBname).get(0).value));
   * List<TCell> versions = handler.getVer(tableAname, rowAname, columnBname, MAXVERSIONS);
   * assertTrue(Bytes.equals(valueCname, versions.get(0).value));
   * assertTrue(Bytes.equals(valueBname, versions.get(1).value));
   * // Assert that changes were made to rowB
   * TRowResult rowResult2 = handler.getRow(tableAname, rowBname).get(0);
   * assertTrue(Bytes.equals(rowBname, rowResult2.row));
   * assertTrue(Bytes.equals(valueCname, rowResult2.columns.get(columnAname).value));
   * assertTrue(Bytes.equals(valueDname, rowResult2.columns.get(columnBname).value));
   * // Apply some deletes
   * handler.deleteAll(tableAname, rowAname, columnBname);
   * handler.deleteAllRow(tableAname, rowBname);
   * // Assert that the deletes were applied
   * int size = handler.get(tableAname, rowAname, columnBname).size();
   * assertEquals(0, size);
   * size = handler.getRow(tableAname, rowBname).size();
   * assertEquals(0, size);
   * // Teardown
   * handler.disableTable(tableAname);
   * handler.deleteTable(tableAname);
   * }
   * /**
   * Similar to testTableMutations(), except Mutations are applied with
   * specific timestamps and data retrieval uses these timestamps to
   * extract specific versions of data.
   * 
   * @throws Exception
   *           public void doTestTableTimestampsAndColumns() throws Exception {
   *           // Setup
   *           ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler();
   *           handler.createTable(tableAname, getColumnDescriptors());
   *           // Apply timestamped Mutations to rowA
   *           long time1 = System.currentTimeMillis();
   *           handler.mutateRowTs(tableAname, rowAname, getMutations(), time1);
   *           Thread.sleep(1000);
   *           // Apply timestamped BatchMutations for rowA and rowB
   *           long time2 = System.currentTimeMillis();
   *           handler.mutateRowsTs(tableAname, getBatchMutations(), time2);
   *           // Apply an overlapping timestamped mutation to rowB
   *           handler.mutateRowTs(tableAname, rowBname, getMutations(), time2);
   *           // the getVerTs is [inf, ts) so you need to increment one.
   *           time1 += 1;
   *           time2 += 2;
   *           // Assert that the timestamp-related methods retrieve the correct data
   *           assertEquals(2, handler.getVerTs(tableAname, rowAname, columnBname, time2,
   *           MAXVERSIONS).size());
   *           assertEquals(1, handler.getVerTs(tableAname, rowAname, columnBname, time1,
   *           MAXVERSIONS).size());
   *           TRowResult rowResult1 = handler.getRowTs(tableAname, rowAname, time1).get(0);
   *           TRowResult rowResult2 = handler.getRowTs(tableAname, rowAname, time2).get(0);
   *           // columnA was completely deleted
   *           //assertTrue(Bytes.equals(rowResult1.columns.get(columnAname).value, valueAname));
   *           assertTrue(Bytes.equals(rowResult1.columns.get(columnBname).value, valueBname));
   *           assertTrue(Bytes.equals(rowResult2.columns.get(columnBname).value, valueCname));
   *           // ColumnAname has been deleted, and will never be visible even with a getRowTs()
   *           assertFalse(rowResult2.columns.containsKey(columnAname));
   *           List<byte[]> columns = new ArrayList<byte[]>();
   *           columns.add(columnBname);
   *           rowResult1 = handler.getRowWithColumns(tableAname, rowAname, columns).get(0);
   *           assertTrue(Bytes.equals(rowResult1.columns.get(columnBname).value, valueCname));
   *           assertFalse(rowResult1.columns.containsKey(columnAname));
   *           rowResult1 = handler.getRowWithColumnsTs(tableAname, rowAname, columns, time1).get(0);
   *           assertTrue(Bytes.equals(rowResult1.columns.get(columnBname).value, valueBname));
   *           assertFalse(rowResult1.columns.containsKey(columnAname));
   *           // Apply some timestamped deletes
   *           // this actually deletes _everything_.
   *           // nukes everything in columnB: forever.
   *           handler.deleteAllTs(tableAname, rowAname, columnBname, time1);
   *           handler.deleteAllRowTs(tableAname, rowBname, time2);
   *           // Assert that the timestamp-related methods retrieve the correct data
   *           int size = handler.getVerTs(tableAname, rowAname, columnBname, time1, MAXVERSIONS).size();
   *           assertEquals(0, size);
   *           size = handler.getVerTs(tableAname, rowAname, columnBname, time2, MAXVERSIONS).size();
   *           assertEquals(1, size);
   *           // should be available....
   *           assertTrue(Bytes.equals(handler.get(tableAname, rowAname, columnBname).get(0).value, valueCname));
   *           assertEquals(0, handler.getRow(tableAname, rowBname).size());
   *           // Teardown
   *           handler.disableTable(tableAname);
   *           handler.deleteTable(tableAname);
   *           }
   *           /**
   *           Tests the four different scanner-opening methods (with and without
   *           a stoprow, with and without a timestamp).
   * @throws Exception
   *           public void doTestTableScanners() throws Exception {
   *           // Setup
   *           ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler();
   *           handler.createTable(tableAname, getColumnDescriptors());
   *           // Apply timestamped Mutations to rowA
   *           long time1 = System.currentTimeMillis();
   *           handler.mutateRowTs(tableAname, rowAname, getMutations(), time1);
   *           // Sleep to assure that 'time1' and 'time2' will be different even with a
   *           // coarse grained system timer.
   *           Thread.sleep(1000);
   *           // Apply timestamped BatchMutations for rowA and rowB
   *           long time2 = System.currentTimeMillis();
   *           handler.mutateRowsTs(tableAname, getBatchMutations(), time2);
   *           time1 += 1;
   *           // Test a scanner on all rows and all columns, no timestamp
   *           int scanner1 = handler.scannerOpen(tableAname, rowAname, getColumnList(true, true));
   *           TRowResult rowResult1a = handler.scannerGet(scanner1).get(0);
   *           assertTrue(Bytes.equals(rowResult1a.row, rowAname));
   *           // This used to be '1'. I don't know why when we are asking for two columns
   *           // and when the mutations above would seem to add two columns to the row.
   *           // -- St.Ack 05/12/2009
   *           assertEquals(rowResult1a.columns.size(), 1);
   *           assertTrue(Bytes.equals(rowResult1a.columns.get(columnBname).value, valueCname));
   *           TRowResult rowResult1b = handler.scannerGet(scanner1).get(0);
   *           assertTrue(Bytes.equals(rowResult1b.row, rowBname));
   *           assertEquals(rowResult1b.columns.size(), 2);
   *           assertTrue(Bytes.equals(rowResult1b.columns.get(columnAname).value, valueCname));
   *           assertTrue(Bytes.equals(rowResult1b.columns.get(columnBname).value, valueDname));
   *           closeScanner(scanner1, handler);
   *           // Test a scanner on all rows and all columns, with timestamp
   *           int scanner2 = handler.scannerOpenTs(tableAname, rowAname, getColumnList(true, true), time1);
   *           TRowResult rowResult2a = handler.scannerGet(scanner2).get(0);
   *           assertEquals(rowResult2a.columns.size(), 1);
   *           // column A deleted, does not exist.
   *           //assertTrue(Bytes.equals(rowResult2a.columns.get(columnAname).value, valueAname));
   *           assertTrue(Bytes.equals(rowResult2a.columns.get(columnBname).value, valueBname));
   *           closeScanner(scanner2, handler);
   *           // Test a scanner on the first row and first column only, no timestamp
   *           int scanner3 = handler.scannerOpenWithStop(tableAname, rowAname, rowBname,
   *           getColumnList(true, false));
   *           closeScanner(scanner3, handler);
   *           // Test a scanner on the first row and second column only, with timestamp
   *           int scanner4 = handler.scannerOpenWithStopTs(tableAname, rowAname, rowBname,
   *           getColumnList(false, true), time1);
   *           TRowResult rowResult4a = handler.scannerGet(scanner4).get(0);
   *           assertEquals(rowResult4a.columns.size(), 1);
   *           assertTrue(Bytes.equals(rowResult4a.columns.get(columnBname).value, valueBname));
   *           // Teardown
   *           handler.disableTable(tableAname);
   *           handler.deleteTable(tableAname);
   *           }
   *           /**
   * @return a List of ColumnDescriptors for use in creating a table. Has one
   *         default ColumnDescriptor and one ColumnDescriptor with fewer versions
   *         private List<ColumnDescriptor> getColumnDescriptors() {
   *         ArrayList<ColumnDescriptor> cDescriptors = new ArrayList<ColumnDescriptor>();
   *         // A default ColumnDescriptor
   *         ColumnDescriptor cDescA = new ColumnDescriptor();
   *         cDescA.name = columnAname;
   *         cDescriptors.add(cDescA);
   *         // A slightly customized ColumnDescriptor (only 2 versions)
   *         ColumnDescriptor cDescB = new ColumnDescriptor(columnBname, 2, "NONE",
   *         false, "NONE", 0, 0, false, -1);
   *         cDescriptors.add(cDescB);
   *         return cDescriptors;
   *         }
   *         /**
   * @param includeA whether or not to include columnA
   * @param includeB whether or not to include columnB
   * @return a List of column names for use in retrieving a scanner
   *         private List<byte[]> getColumnList(boolean includeA, boolean includeB) {
   *         List<byte[]> columnList = new ArrayList<byte[]>();
   *         if (includeA) columnList.add(columnAname);
   *         if (includeB) columnList.add(columnBname);
   *         return columnList;
   *         }
   *         /**
   * @return a List of Mutations for a row, with columnA having valueA
   *         and columnB having valueB
   *         private List<Mutation> getMutations() {
   *         List<Mutation> mutations = new ArrayList<Mutation>();
   *         mutations.add(new Mutation(false, columnAname, valueAname));
   *         mutations.add(new Mutation(false, columnBname, valueBname));
   *         return mutations;
   *         }
   *         /**
   * @return a List of BatchMutations with the following effects:
   *         (rowA, columnA): delete
   *         (rowA, columnB): place valueC
   *         (rowB, columnA): place valueC
   *         (rowB, columnB): place valueD
   *         private List<BatchMutation> getBatchMutations() {
   *         List<BatchMutation> batchMutations = new ArrayList<BatchMutation>();
   *         // Mutations to rowA. You can't mix delete and put anymore.
   *         List<Mutation> rowAmutations = new ArrayList<Mutation>();
   *         rowAmutations.add(new Mutation(true, columnAname, null));
   *         batchMutations.add(new BatchMutation(rowAname, rowAmutations));
   *         rowAmutations = new ArrayList<Mutation>();
   *         rowAmutations.add(new Mutation(false, columnBname, valueCname));
   *         batchMutations.add(new BatchMutation(rowAname, rowAmutations));
   *         // Mutations to rowB
   *         List<Mutation> rowBmutations = new ArrayList<Mutation>();
   *         rowBmutations.add(new Mutation(false, columnAname, valueCname));
   *         rowBmutations.add(new Mutation(false, columnBname, valueDname));
   *         batchMutations.add(new BatchMutation(rowBname, rowBmutations));
   *         return batchMutations;
   *         }
   *         /**
   *         Asserts that the passed scanner is exhausted, and then closes
   *         the scanner.
   * @param scannerId the scanner to close
   * @param handler the HBaseHandler interfacing to HBase
   * @throws Exception
   *           private void closeScanner(int scannerId, ThriftServer.HBaseHandler handler) throws Exception {
   *           handler.scannerGet(scannerId);
   *           handler.scannerClose(scannerId);
   *           }
   */
}
