/*
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

// NOTE: The "required" and "optional" keywords for the service methods are purely for documentation

namespace java org.apache.hadoop.hbase.thrift2.generated
namespace cpp apache.hadoop.hbase.thrift2
namespace rb Apache.Hadoop.Hbase.Thrift2
namespace py hbase
namespace perl Hbase

include "Types.thrift"

service HbaseClient {

  /**
   * Test for the existence of columns in the table, as specified in the TGet.
   *
   * @return true if the specified TGet matches one or more keys, false if not
   */
  bool exists(
    /** the table to check on */
    1: required binary table,

    /** the TGet to check for */
    2: required Types.TGet get
  ) throws (1:Types.TIOError io)

  /**
   * Method for getting data from a row.
   *
   * If the row cannot be found an empty Result is returned.
   * This can be checked by the empty field of the TResult
   *
   * @return the result
   */
  Types.TResult get(
    /** the table to get from */
    1: required binary table,

    /** the TGet to fetch */
    2: required Types.TGet get
  ) throws (1: Types.TIOError io)

  /**
   * Method for getting multiple rows.
   *
   * If a row cannot be found there will be a null
   * value in the result list for that TGet at the
   * same position.
   *
   * So the Results are in the same order as the TGets.
   */
  list<Types.TResult> getMultiple(
    /** the table to get from */
    1: required binary table,

    /** a list of TGets to fetch, the Result list
        will have the Results at corresponding positions
        or null if there was an error */
    2: required list<Types.TGet> gets
  ) throws (1: Types.TIOError io)

  /**
   * Return the row that matches <i>row</i> exactly,
   * or the one that immediately precedes it.
   */
  Types.TResult getRowOrBefore(
    /** the table to get from */
    1: required binary table,

    /** the row key to get or the one preceding it */
    2: required binary row,

    /** the column family to get */
    3: required binary family
  ) throws (1: Types.TIOError io)

  /**
   * Commit a TPut to a table.
   */
  void put(
    /** the table to put data in */
    1: required binary table,

    /** the TPut to put */
    2: required Types.TPut put
  ) throws (1: Types.TIOError io)

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the TPut.
   *
   * @return true if the new put was executed, false otherwise
   */
  bool checkAndPut(
    /** to check in and put to */
    1: required binary table,

    /** row to check */
    2: required binary row,

    /** column family to check */
    3: required binary family,

    /** column qualifier to check */
    4: required binary qualifier,

    /** the expected value, if not provided the
        check is for the non-existence of the
        column in question */
    5: optional binary value,

    /** the TPut to put if the check succeeds */
    6: required Types.TPut put
  ) throws (1: Types.TIOError io)

  /**
   * Commit a List of Puts to the table.
   */
  void putMultiple(
    /** the table to put data in */
    1: required binary table,

    /** a list of TPuts to commit */
    2: required list<Types.TPut> puts
  ) throws (1: Types.TIOError io)

  /**
   * Deletes as specified by the TDelete.
   *
   * Note: "delete" is a reserved keyword and cannot be used in Thrift
   * thus the inconsistent naming scheme from the other functions.
   */
  void deleteSingle(
    /** the table to delete from */
    1: required binary table,

    /** the TDelete to delete */
    2: required Types.TDelete deleteSingle
  ) throws (1: Types.TIOError io)

  /**
   * Bulk commit a List of TDeletes to the table.
   *
   * This returns a list of TDeletes that were not
   * executed. So if everything succeeds you'll
   * receive an empty list.
   */
  list<Types.TDelete> deleteMultiple(
    /** the table to delete from */
    1: required binary table,

    /** list of TDeletes to delete */
    2: required list<Types.TDelete> deletes
  ) throws (1: Types.TIOError io)

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.
   *
   * @return true if the new delete was executed, false otherwise
   */
  bool checkAndDelete(
    /** to check in and delete from */
    1: required binary table,

    /** row to check */
    2: required binary row,

    /** column family to check */
    3: required binary family,

    /** column qualifier to check */
    4: required binary qualifier,

    /** the expected value, if not provided the
        check is for the non-existence of the
        column in question */
    5: optional binary value,

    /** the TDelete to execute if the check succeeds */
    6: required Types.TDelete deleteSingle
  ) throws (1: Types.TIOError io)

  /**
   * Atomically increments a single column by a user provided amount.
   */
  i64 incrementColumnValue(
    /** the table to increment the value on */
    1: required binary table,

    /** the row where the value should be incremented */
    2: required binary row,

    /** the family in the row where the value should be incremented */
    3: required binary family,

    /** the column qualifier where the value should be incremented */
    4: required binary qualifier,

    /** the amount by which the value should be incremented */
    5: optional i64 amount = 1,

    /** if this increment should be written to the WAL or not */
    6: optional bool writeToWal = 1
  ) throws (1: Types.TIOError io)

  /**
   * Get a Scanner for the provided TScan object.
   *
   * @return Scanner Id to be used with other scanner procedures
   */
  i32 openScanner(
    /** the table to get the Scanner for */
    1: required binary table,

    /** the scan object to get a Scanner for */
    2: required Types.TScan scan,
  ) throws (1: Types.TIOError io)

  /**
   * Grabs multiple rows from a Scanner.
   *
   * @return Between zero and numRows TResults
   */
  list<Types.TResult> getScannerRows(
    /** the Id of the Scanner to return rows from. This is an Id returned from the openScanner function. */
    1: required i32 scannerId,

    /** number of rows to return */
    2: optional i32 numRows = 1
  ) throws (
    1: Types.TIOError io,

    /** if the scannerId is invalid */
    2: Types.TIllegalArgument ia
  )

  /**
   * Closes the scanner. Should be called if you need to close
   * the Scanner before all results are read.
   *
   * Exhausted scanners are closed automatically.
   */
  void closeScanner(
    /** the Id of the Scanner to close **/
    1: required i32 scannerId
  ) throws (
    1: Types.TIOError io,

    /** if the scannerId is invalid */
    2: Types.TIllegalArgument ia
  )

}
