package org.apache.hadoop.hbase.thrift2;

import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is a glue object that connects Thrift RPC calls to the HBase client API primarily defined in the
 * HTableInterface.
 */
public class ThriftHBaseServiceHandler implements THBaseService.Iface {

  // TODO: Size of pool configuraple
  private final HTablePool htablePool = new HTablePool();
  private static final Log LOG = LogFactory.getLog(ThriftHBaseServiceHandler.class);

  // nextScannerId and scannerMap are used to manage scanner state
  // TODO: Cleanup thread for Scanners, Scanner id wrap
  private final AtomicInteger nextScannerId = new AtomicInteger(0);
  private final Map<Integer, ResultScanner> scannerMap = new ConcurrentHashMap<Integer, ResultScanner>();

  private HTableInterface getTable(byte[] tableName) {
    return htablePool.getTable(tableName);
  }

  private void putTable(HTableInterface table) throws TIOError {
    try {
      htablePool.putTable(table);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  private TIOError getTIOError(IOException e) {
    TIOError err = new TIOError();
    err.setMessage(e.getMessage());
    return err;
  }

  /**
   * Assigns a unique ID to the scanner and adds the mapping to an internal HashMap.
   * 
   * @param scanner to add
   * 
   * @return Id for this Scanner
   */
  private int addScanner(ResultScanner scanner) {
    int id = nextScannerId.getAndIncrement();
    scannerMap.put(id, scanner);
    return id;
  }

  /**
   * Returns the Scanner associated with the specified Id.
   * 
   * @param id of the Scanner to get
   * 
   * @return a Scanner, or null if the Id is invalid
   */
  private ResultScanner getScanner(int id) {
    return scannerMap.get(id);
  }

  /**
   * Removes the scanner associated with the specified ID from the internal HashMap.
   * 
   * @param id of the Scanner to remove
   * 
   * @return the removed Scanner, or <code>null</code> if the Id is invalid
   */
  protected ResultScanner removeScanner(int id) {
    return scannerMap.remove(id);
  }

  @Override
  public boolean exists(ByteBuffer table, TGet get) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      return htable.exists(getFromThrift(get));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public TResult get(ByteBuffer table, TGet get) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      return resultFromHBase(htable.get(getFromThrift(get)));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public List<TResult> getMultiple(ByteBuffer table, List<TGet> gets) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      return resultsFromHBase(htable.get(getsFromThrift(gets)));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public TResult getRowOrBefore(ByteBuffer table, ByteBuffer row, ByteBuffer family) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      return resultFromHBase(htable.getRowOrBefore(row.array(), family.array()));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public void put(ByteBuffer table, TPut put) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      htable.put(putFromThrift(put));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public boolean checkAndPut(ByteBuffer table, ByteBuffer row, ByteBuffer family, ByteBuffer qualifier,
                             ByteBuffer value, TPut put) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      return htable.checkAndPut(row.array(), family.array(), qualifier.array(), (value == null) ? null : value.array(), putFromThrift(put));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public void putMultiple(ByteBuffer table, List<TPut> puts) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      htable.put(putsFromThrift(puts));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public void deleteSingle(ByteBuffer table, TDelete deleteSingle) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      htable.delete(deleteFromThrift(deleteSingle));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public List<TDelete> deleteMultiple(ByteBuffer table, List<TDelete> deletes) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    List<Delete> tempDeletes = deletesFromThrift(deletes);
    try {
      htable.delete(tempDeletes);
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
    return deletesFromHBase(tempDeletes);
  }

  @Override
  public boolean checkAndDelete(ByteBuffer table, ByteBuffer row, ByteBuffer family, ByteBuffer qualifier,
                                ByteBuffer value, TDelete deleteSingle) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());

    try {
      if (value == null) {
        return htable.checkAndDelete(row.array(), family.array(), qualifier.array(), null,
            deleteFromThrift(deleteSingle));
      } else {
        return htable.checkAndDelete(row.array(), family.array(), qualifier.array(), value.array(),
            deleteFromThrift(deleteSingle));
      }
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public long incrementColumnValue(ByteBuffer table, ByteBuffer row, ByteBuffer family, ByteBuffer qualifier,
                                   long amount, boolean writeToWal) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      return htable.incrementColumnValue(row.array(), family.array(), qualifier.array(), amount, writeToWal);
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public TResult increment(ByteBuffer table, TIncrement increment) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    try {
      return resultFromHBase(htable.increment(incrementFromThrift(increment)));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
  }

  @Override
  public int openScanner(ByteBuffer table, TScan scan) throws TIOError, TException {
    HTableInterface htable = getTable(table.array());
    ResultScanner resultScanner = null;
    try {
      resultScanner = htable.getScanner(scanFromThrift(scan));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      putTable(htable);
    }
    return addScanner(resultScanner);
  }

  @Override
  public List<TResult> getScannerRows(int scannerId, int numRows) throws TIOError, TIllegalArgument, TException {
    ResultScanner scanner = getScanner(scannerId);
    if (scanner == null) {
      TIllegalArgument ex = new TIllegalArgument();
      ex.setMessage("Invalid scanner Id");
      throw ex;
    }

    try {
      return resultsFromHBase(scanner.next(numRows));
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public void closeScanner(int scannerId) throws TIOError, TIllegalArgument, TException {
    if (removeScanner(scannerId) == null) {
      TIllegalArgument ex = new TIllegalArgument();
      ex.setMessage("Invalid scanner Id");
      throw ex;
    }
  }

}
