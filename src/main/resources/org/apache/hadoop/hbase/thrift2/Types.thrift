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

namespace java org.apache.hadoop.hbase.thrift2.generated
namespace cpp apache.hadoop.hbase.thrift2
namespace rb Apache.Hadoop.Hbase.Thrift2
namespace py hbase
namespace perl Hbase

const string VERSION = "2.0.0"

struct TTimeRange {
  1: required i64 minStamp,
  2: required i64 maxStamp
}

/**
 * Addresses a single cell or multiple cells
 * in a HBase table by column family and optionally
 * a column qualifier and timestamp
 */
struct TColumn {
  1: required binary family,
  2: optional binary qualifier,
  3: optional i64 timestamp
}

/**
 * Represents a single cell and its value.
 */
struct TColumnValue {
  1: required binary family,
  2: required binary qualifier,
  3: required binary value,
  4: optional i64 timestamp
}

/**
 * TODO: optional bool "empty"?
 */
struct TResult {
  1: required binary row,
  2: required list<TColumnValue> entries
}

/**
 * Used to perform Get operations on a single row.
 *
 * The scope can be further narrowed down by specifying a list of
 * columns or column families.
 *
 * To get everything for a row, instantiate a Get object with just the row to get.
 * To further define the scope of what to get you can add a timestamp or time range
 * with an optional maximum number of versions to return.
 *
 * If you specify a time range and a timestamp the range is ignored.
 * Timestamps on TColumns are ignored.
 *
 * TODO: Filter, Locks
 */
struct TGet {
  1: required binary row,
  2: optional list<TColumn> columns,

  3: optional i64 timestamp,
  4: optional TTimeRange timeRange,

  5: optional i32 maxVersions,
}

/**
 * Used to perform Put operations for a single row.
 *
 * Add column values to this object and they'll be added.
 * You can provide a default timestamp if the column values
 * don't have one. If you don't provide a default timestamp
 * the current time is inserted.
 *
 * You can also define it this Put should be written
 * to the write-ahead Log (WAL) or not. It defaults to true.
 */
struct TPut {
  1: required binary row,
  2: required list<TColumnValue> columnValues
  3: optional i64 timestamp,
  4: optional bool writeToWal = 1
}

/**
 * Used to perform Delete operations on a single row.
 *
 * The scope can be further narrowed down by specifying a list of
 * columns or column families as TColumns.
 *
 * Specifying only a family in a TColumn will delete the whole family.
 * If a timestamp is specified all versions with a timestamp less than
 * or equal to this will be deleted. If no timestamp is specified the
 * current time will be used.
 *
 * Specifying a family and a column qualifier in a TColumn will delete only
 * this qualifier. If a timestamp is specified only versions equal
 * to this timestamp will be deleted. If no timestamp is specified the
 * most recent version will be deleted.
 *
 * The top level timestamp is only used if a complete row should be deleted
 * (i.e. no columns are passed) and if it is specified it works the same way
 * as if you had added a TColumn for every column family and this timestamp
 * (i.e. all versions older than or equal in all column families will be deleted)
 *
 * TODO: This is missing the KeyValue.Type.DeleteColumn semantic. I could add a DeleteType or something like that
 */
struct TDelete {
  1: required binary row,
  2: optional list<TColumn> columns
  3: optional i64 timestamp
}

struct TIncrement {
  1: required binary row,
  2: required map<TColumn, i64> columns
}

/**
 * TODO: Filter
 */
struct TScan {
  1: optional binary startRow,
  2: optional binary stopRow,
  3: optional list<TColumn> columns
  4: optional i32 caching,
  5: optional i32 maxVersions,
  6: optional TTimeRange timeRange,
}

//
// Exceptions
//

/**
 * A TIOError exception signals that an error occurred communicating
 * to the HBase master or a HBase region server. Also used to return
 * more general HBase error conditions.
 */
exception TIOError {
  1: optional string message
}

/**
 * A TIllegalArgument exception indicates an illegal or invalid
 * argument was passed into a procedure.
 */
exception TIllegalArgument {
  1: optional string message
}
