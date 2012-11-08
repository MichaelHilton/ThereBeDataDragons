
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.sql.*;
import java.util.*;


public class Accumulo_Scan_A_Row_Driver {
	
	public static void readMysql(){
		
		
		 Connection conn = null;
	        Statement stmt = null;
	        ResultSet rs = null;

	        try {
	            String dbURL = "jdbc:mysql://localhost/Mathzor";
	            String username = "root";
	            String password = "hilton";

	            Class.forName("com.mysql.jdbc.Driver");

	            conn =
	                DriverManager.getConnection(dbURL, username, password);

	            stmt = conn.createStatement();

	            if (stmt.execute("select * from quizzes")) {
	                rs = stmt.getResultSet();
	            } else {
	                System.err.println("select failed");
	            }
	            while (rs.next()) {
	                String entry = rs.getString(1);
	                System.out.println(rs.getString(1)+" "+rs.getString(2)+" "+rs.getString(3)+" "+rs.getString(4)+" "+rs.getString(5)+" "+rs.getString(6)+" "+rs.getString(7));
	            }

	        } catch (ClassNotFoundException ex) {
	            System.err.println("Failed to load mysql driver");
	            System.err.println(ex);
	        } catch (SQLException ex) {
	            System.out.println("SQLException: " + ex.getMessage()); 
	            System.out.println("SQLState: " + ex.getSQLState()); 
	            System.out.println("VendorError: " + ex.getErrorCode()); 
	        } finally {
	            if (rs != null) {
	                try {
	                    rs.close();
	                } catch (SQLException ex) { /* ignore */ }
	                rs = null;
	            }
	            if (stmt != null) {
	                try {
	                    stmt.close();
	                } catch (SQLException ex) { /* ignore */ }
	                stmt = null;
	            }
	            if (conn != null) {
	                try {
	                    conn.close();
	                } catch (SQLException ex) { /* ignore */ }
	                conn = null;
	            }
	        }
		
		
		
		
		
		
	}
	

 public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
 
	 //YOU HAVE TO CREATE THE TABLE IN THE SHELL BEFORE YOU RUN I THINK
	 
	 // Accumulo CONNECTION STRING
	 String instanceName = "instance";
	   String zooKeepers = "localhost";
	   String user = "root";
	   byte[] pass = "secret".getBytes();
	   String tableName = "t4";
	   

	   ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
	   Connector connector = instance.getConnector(user, pass);
	  
	   MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(new BatchWriterConfig());
	   BatchWriter bw = null;
	   
	   if (!connector.tableOperations().exists(tableName))
	     connector.tableOperations().create(tableName);
	   bw = mtbw.getBatchWriter(tableName);
	 
	 
	 //MYSQL SETUP
	 Connection conn = null;
     Statement stmt = null;
     ResultSet rs = null;

     try {
         String dbURL = "jdbc:mysql://localhost/Mathzor";
         String username = "root";
         String password = "hilton";

         Class.forName("com.mysql.jdbc.Driver");

         conn =
             DriverManager.getConnection(dbURL, username, password);

         stmt = conn.createStatement();

         Text colf = new Text("colfam");
         int even = 0;
         if (stmt.execute("select * from quizzes")) {
             rs = stmt.getResultSet();
         } else {
             System.err.println("select failed");
         }
         while (rs.next()) {
             String entry = rs.getString(1);
             System.out.println(rs.getString(1)+" "+rs.getString(2)+" "+rs.getString(3)+" "+rs.getString(4)+" "+rs.getString(5)+" "+rs.getString(6)+" "+rs.getString(7));
 
             if(rs.getString(5) != null){
             
             //insert records into Accumulo
             Mutation m = new Mutation(rs.getString(1));
             //for (int j = 0; j < 5; j++) {
            
            	 /*
            	  * 
            	  *  m.put(colf, new Text(String.format("colqual_%d", j)), new Value((String.format("value_%d_%d", i, j)).getBytes()));
            	  * */
            	 /*
            	  * T1 setup
            	 Value v =  new Value((String.format("0")).getBytes()) ;
            	 m.put(new Text("qStudent"), new Text(rs.getString(2)), v) ;
            	 m.put(new Text("stamp"), new Text(rs.getString(3)), v) ;
            	 m.put(new Text("level"), new Text(rs.getString(4)), v) ;
            	 m.put(new Text("mathFuncht"), new Text(rs.getString(5)), v) ;
            	 m.put(new Text("percent"), new Text(rs.getString(6)), v) ;
            	 m.put(new Text("testTime"), new Text(rs.getString(7)), v) ;
            	*/
            	 
             Text t = new Text("Colq") ;
             //T2 setup
        	 //Value v =  new Value((String.format("0")).getBytes()) ;
             ColumnVisibility cviz = null ;
             if(even > 0){
             cviz = new ColumnVisibility("student") ;
             even = 0;
             }else{
             cviz = new ColumnVisibility("student&teacher") ;
             even = 1;
             }
             Value v =  new Value((String.format("0")).getBytes()) ;
        	 m.put(new Text("qStudent"), new Text(rs.getString(2)),cviz, v) ;
        	 m.put(new Text("stamp"), new Text(rs.getString(3)),cviz, v) ;
        	 m.put(new Text("level"), new Text(rs.getString(4)),cviz, v) ;
        	 m.put(new Text("mathFuncht"), new Text(rs.getString(5)),cviz, v) ;
        	 m.put(new Text("percent"), new Text(rs.getString(6)),cviz, v) ;
        	 m.put(new Text("testTime"), new Text(rs.getString(7)),cviz, v) ;
             
             /*m.put(new Text("qStudent"), t,cviz, new Value((rs.getString(2)).getBytes())) ;
        	 m.put(new Text("stamp"), t,cviz, new Value((rs.getString(3)).getBytes())) ;
        	 m.put(new Text("level"), t,cviz, new Value((rs.getString(4)).getBytes())) ;
        	 m.put(new Text("mathFuncht"), t,cviz, new Value((rs.getString(5)).getBytes())) ;
        	 m.put(new Text("percent"), t,cviz, new Value((rs.getString(6)).getBytes())) ;
        	 m.put(new Text("testTime"), t,cviz, new Value((rs.getString(7)).getBytes())) ;
        	*/
             
            	 // m.put(colf, new Text("qStudent"), new Text(rs.getString(2)) ;
            	 
            	 
             //  m.put(colf, new Text("qStudent"), new Text(rs.getString(2)) ;
               //m.put(new Text(rs.getString(2)), new Text(String.format("colqual_%d", j)), new Value((String.format("value_%d_%d", i, j)).getBytes()));
             //}
             bw.addMutation(m);
             }
         }
         mtbw.close();

     } catch (ClassNotFoundException ex) {
         System.err.println("Failed to load mysql driver");
         System.err.println(ex);
     } catch (SQLException ex) {
         System.out.println("SQLException: " + ex.getMessage()); 
         System.out.println("SQLState: " + ex.getSQLState()); 
         System.out.println("VendorError: " + ex.getErrorCode()); 
     } finally {
         if (rs != null) {
             try {
                 rs.close();
             } catch (SQLException ex) { /* ignore */ }
             rs = null;
         }
         if (stmt != null) {
             try {
                 stmt.close();
             } catch (SQLException ex) { /* ignore */ }
             stmt = null;
         }
         if (conn != null) {
             try {
                 conn.close();
             } catch (SQLException ex) { /* ignore */ }
             conn = null;
         }
     }
	
	
	
	 
	 
	// readMysql();

	 
  //ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
  //Connector connector = instance.getConnector(user, pass);
 /* MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(new BatchWriterConfig());
  
  BatchWriter bw = null;
  
  if (!connector.tableOperations().exists(tableName))
    connector.tableOperations().create(tableName);
  bw = mtbw.getBatchWriter(tableName);
  
  Text colf = new Text("colfam");
  System.out.println("writing ...");
  for (int i = 0; i < 10000; i++) {
    Mutation m = new Mutation(new Text(String.format("row_%d", i)));
    for (int j = 0; j < 5; j++) {
      m.put(colf, new Text(String.format("colqual_%d", j)), new Value((String.format("value_%d_%d", i, j)).getBytes()));
    }
    bw.addMutation(m);
    if (i % 100 == 0)
      System.out.println(i);
  }
  
  mtbw.close();
*/
  /*
  Scanner scan = connector.createScanner(tableName, new Authorizations());
  scan.setRange(new Range(rowId, rowId));

  Iterator<Map.Entry<Key,Value>> iterator = scan.iterator();
  while (iterator.hasNext()) {
   Map.Entry<Key,Value> entry = iterator.next();
   Key key = entry.getKey();
   Value value = entry.getValue();
   System.out.println(key + " ==> " + value);
   
  }
  */
  
  /*
  Scanner scan = connector.createScanner(tableName, Constants.NO_AUTHS);
  Key start = null;
  Key end = null;
  scan.setRange(new Range(start, end));
  Iterator<Entry<Key,Value>> iter = scan.iterator();
  
  while (iter.hasNext()) {
    Entry<Key,Value> e = iter.next();
    Text colf = e.getKey().getColumnFamily();
    Text colq = e.getKey().getColumnQualifier();
    System.out.print("row: " + e.getKey().getRow() + ", colf: " + colf + ", colq: " + colq);
    System.out.println(", value: " + e.getValue().toString());
  }
  
  */

 } 
}
