package org.zephyr.hbase.tool.replication;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SimulateMutateTable {
  private static final Log LOG = LogFactory.getLog(SimulateMutateTable.class);
  
  private final String TABLE = "_SyncTest_TABLE_";
  private final String NAMESPACE = "_SyncTest_NAMESPACE_";
  private final byte[] COLUMN = Bytes.toBytes("F");
  private final byte[][] SPLITS = {Bytes.toBytes("0010"), Bytes.toBytes("0100"), Bytes.toBytes("1000")};
  private Connection con;
  private Admin admin;
  private boolean run = true;
  private List<HTableDescriptor> tables = new LinkedList<HTableDescriptor>();
  
  public SimulateMutateTable(Connection con) throws IOException {
    this.con = con;
    setup(con.getAdmin());
  }
  
  private void setup(Admin admin) throws IOException {
    this.admin = admin;
    admin.createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(NAMESPACE, TABLE));
    HColumnDescriptor family = new HColumnDescriptor(COLUMN);
    htd.addFamily(family); 
    admin.createTable(htd, SPLITS);
    tables.add(htd);
  }
  
  public HTableDescriptor[] getTableList() {
    return tables.toArray(new HTableDescriptor[tables.size()]);
  }
  
  public void stop() {
    run = false;
  }
  
  public void close() throws IOException {
    admin.close();
  }
  
  public void clear(Admin dstAdmin) throws IOException {
    TableName testTable = TableName.valueOf(NAMESPACE, TABLE);
    if (dstAdmin.tableExists(testTable)) {
      dstAdmin.disableTable(testTable);
      dstAdmin.deleteTable(testTable);
      dstAdmin.deleteNamespace(NAMESPACE);
    }
    if (admin.tableExists(testTable)) {
      admin.disableTable(testTable);
      admin.deleteTable(testTable);
      admin.deleteNamespace(NAMESPACE);
    }
  }
  
  public void start() {
    Executor pool = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 5; i++) {
      pool.execute(new MutateTask(i));
    }
  }
  
  class MutateTask implements Runnable {
    
    String name;
    
    public MutateTask(int i) {
      name = "Mutation-task-" + i;
    }
    
    @Override
    public void run() {
      Table table = null;
      try {        
        try {
          table = con.getTable(TableName.valueOf(NAMESPACE, TABLE));
        } catch (IOException e) {
          LOG.error("getTable has failed in " + name, e);
          return ;
        }
        
        LOG.info(name + " start.");
        
        Random rand = new Random();
      
        while(run){
          int num = rand.nextInt(10000000);
          byte[] x = Bytes.toBytes(String.format("%07d", num));
          Put put = new Put(x);
          put.addColumn(COLUMN, x, x);
          try {
            table.put(put);
          
            try {
              Thread.sleep(rand.nextInt(128));
            } catch (InterruptedException e) {
              LOG.warn(name + " sleep is interrupted.");
            }
            
            if (num % 2 == 0) {
              Delete del = new Delete(x);
              //del.addColumn(COLUMN, x);
              table.delete(del);
            }
          } catch (IOException e) {
            LOG.error(name + " can't do mutation.", e);
            break;
          }
        }
      } catch (RuntimeException e) {
        LOG.error(name + " broken.", e);
      } finally {
        try {
          if (table != null) table.close();
        } catch (IOException e) {
          LOG.error("close table in " + name + " has failed.", e);
        }       
        LOG.info(name + " has stoped.");
      }
    }
    
  }
  
}