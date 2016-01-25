package org.zephyr.hbase.tool.replication;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;


/*
 *  Record the table that success be replicated.
 *  We will exclude these tables that log in special file, when we run SyncHTable tool.
 * 
 */
public class ReplicationManager {
  private static final Log LOG = LogFactory.getFactory().getLog(ReplicationManager.class);
  
  private final String LOGPATH;
  private final static String LOGFILE = "exclude-table";
  private static ReplicationManager instance;
  private final BufferedReader reader;
  private final BufferedWriter writer;
  
  private ReplicationManager() throws IOException {
    LOGPATH = System.getProperty("SyncHTable.log", "./");
    File logPath = new File(LOGPATH);
    if (!logPath.exists()) {
      logPath.mkdirs();
    }
    File f = new File(logPath, LOGFILE);
    if (!f.exists()) {
      f.createNewFile();
    }
    reader = new BufferedReader(new FileReader(f));
    writer = new BufferedWriter(new FileWriter(f));
  }
  
  public static synchronized ReplicationManager create() {
    if (instance == null) try {
      instance = new ReplicationManager();
    } catch (IOException e) {
      LOG.error("can't create ReplicationManager.", e);
      return null;
    }
    return instance;
  }
  
  public void commitSuccessfulTable(TableName tb) {   
    try {
      writer.write(tb.getNameWithNamespaceInclAsString());
    } catch (IOException e) {
      LOG.warn("can't write '" + tb.getNameWithNamespaceInclAsString() + "' in " + LOGPATH + "/"
          + LOGFILE, e);
    }
  }
  
  public List<TableName> listSuccessfulTables() {
    String namespace;
    String qualifier;
    String line;
    List<TableName> returnVal = new LinkedList<TableName>();
    try {
      while ((line = reader.readLine()) != null) {
        String[] args = line.split(":", 2);
        namespace = args[0];
        qualifier = args[1];
        try {
          returnVal.add(TableName.valueOf(namespace, qualifier));
        } catch (IllegalArgumentException e){
          LOG.warn("can't parse '" + line + "' to TableName", e);
        }
      }
    } catch (IOException e) {
      LOG.warn("can't read TableName from " + LOGPATH + "/" + LOGFILE, e);
    }
    return returnVal;
  }
}
