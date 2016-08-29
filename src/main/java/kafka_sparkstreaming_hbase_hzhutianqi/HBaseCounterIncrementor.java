package kafka_sparkstreaming_hbase_hzhutianqi;
  
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import kafka_sparkstreaming_hbase_hzhutianqi.CounterMap.Counter;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;  


public class HBaseCounterIncrementor {
	static HBaseCounterIncrementor singleton;
	static String tableName;
	static String columnFamily;
	static Connection connection = null;
	static Admin admin = null;
//	static FlushThread flushThread;
	static long flushInterval;
//	static CloserThread closerThread;
	static HashMap<String , CounterMap> rowKeyCounterMap = new HashMap<String , CounterMap>();
	static long lastUsed;  
	static  HBaseManagerMain hbaseManagerMain ;
	
	static Object locker = new Object();
	
	/**
	 * 
	 *  @author hzhutianqi
	 *  初始化配置
	 *  init with configuration , connection , admin and hbaseManagerMain
	 *  @param tableName , columnFamily
     *  @throws IOException
     *  
	 */	
	public HBaseCounterIncrementor(String tableName , String columnFamily){
		HBaseCounterIncrementor.tableName = tableName;
		HBaseCounterIncrementor.columnFamily = columnFamily;
		if(connection == null){
			synchronized(locker){
				if(connection == null){
					Configuration hConfig = HBaseConfiguration.create();
			         hConfig.set("hbase.zookeeper.quorum", "10.240.84.15");
			         hConfig.set("hbase.zookeeper.property.clientPort", "2182");
			         try{
			        	 connection = ConnectionFactory.createConnection(hConfig);
			        	 admin = connection.getAdmin();			        	 
			        	 hbaseManagerMain = new HBaseManagerMain(connection , admin);
			        	 
			        	 //list all the tables
			        	 hbaseManagerMain.listTables();
			        	 
			        	//judge whether table exists or not
			            boolean exists = hbaseManagerMain.isExists(tableName);	             
			            
			            //delete the table if exists
			            if (exists) {
			            	hbaseManagerMain.deleteTable(tableName);
			            } 
			           
			            //create the table
			            hbaseManagerMain.createTable(tableName);
			            
			            //list all the tables again
			            hbaseManagerMain.listTables();
			            
			         }catch(IOException e){
			        	 e.printStackTrace();
			        	 throw new RuntimeException(e);
			         }
				}
			}
		}
	}
	
	/**
	 * 
	 * 构造单例
	 *  @param tableName , columnFamily
	 *  
	 */	
	public static HBaseCounterIncrementor getInstance(String tableName , String columnFamily){
		if(singleton == null){
			synchronized(locker){
				if(singleton == null){
					singleton = new HBaseCounterIncrementor(tableName , columnFamily);
				}
			}
		}
		return singleton;
	}
	
	/**
	 * 
	 * 写入HBase
	 *  @IOException
	 *  
	 */	
	private static void flushTOHBase(){
			         try{		            
				        hbaseManagerMain.putDatas(tableName, columnFamily, rowKeyCounterMap);
				        //scan the table
				        hbaseManagerMain.scanTable(tableName);
				        
				        //hbaseManagerMain.getData(tableName);
			         }catch(IOException e){
			        	 e.printStackTrace();
			        	 throw new RuntimeException(e);
			         }
//			         flushThread = new FlushThread(flushInterval);
//			         flushThread.start();
//			         closerThread = new CloserThread();
//			         closerThread.start();
				

	}
	
	public void increment(String rowKey , String key , int increment){
		increment(rowKey , key , (long) increment);
	}
	
	public void increment(String rowKey , String key , long increment){
		CounterMap counterMap = rowKeyCounterMap.get(rowKey);
		if(counterMap == null){
			counterMap = new CounterMap();
			rowKeyCounterMap.put(rowKey , counterMap);
		}
		counterMap.increment(key , increment);
		flushTOHBase();
	}
	
	private static void updateLastUsed(){
		lastUsed = System.currentTimeMillis();
	}
	
//	public static class FlushThread extends Thread{
//		long sleepTime;
//		boolean continueLoop = true;
//		
//		public FlushThread(long sleepTime){
//			this.sleepTime = sleepTime;
//		}
//		
//		public void run(){
//			while(continueLoop){
//				try{
//					flushToHBase();
//				}catch(IOException e){
//					e.printStackTrace();
//					break;
//				}
//				try{
//					Thread.sleep(sleepTime);
//				}catch(InterruptedException e){
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		private void flushToHBase() throws IOException{
//			synchronized(connection){
//				if(connection == null){
//					flushTOHBase();
//				}
//				updateLastUsed();
//				
//				TableName tablename = TableName.valueOf(tableName);
//				byte[] family = Bytes.toBytes(columnFamily);
//				Table table = connection.getTable(tablename);
//				
//				for (Entry<String, CounterMap> entry : rowKeyCounterMap.entrySet()) {  
//			          CounterMap pastCounterMap = entry.getValue();  
//			          rowKeyCounterMap.put(entry.getKey(), new CounterMap());  	
//			          
//			          byte[] rowkey = Bytes.toBytes(entry.getKey());
//			          Put put = new Put(rowkey);
//			        		          
//			          boolean hasColumns = false;  
//			          for (Entry<String, Counter> entry2 : pastCounterMap.entrySet()) {  			        	  
//			        	  byte [] qualifier = Bytes.toBytes(entry2.getKey());
//			        	  byte[] value = Bytes.toBytes(entry2.getValue().value);
//			        	  put.addColumn(family , qualifier , value);		        	  
//			            hasColumns = true;  
//			          }  
//			          if (hasColumns) {  
//			            updateLastUsed();  
//				         table.put(put);
//			          }  
//			        }  
//			        updateLastUsed();  
//			}
//		}
//		public void stopLoop(){
//			continueLoop = false;
//		}	
//	}
//	
//	public static class CloserThread extends Thread{
//		boolean continueLoop = true;
//		
//		public void run(){
//			while(continueLoop){
//				if(System.currentTimeMillis() - lastUsed > 5000){
//					singleton.close();
//					break;
//				}
//				try{
//					Thread.sleep(60000);
//				}catch(InterruptedException e){
//					e.printStackTrace();
//				}
//			}
//		}
//		public void stopLoop(){
//			continueLoop = false;
//		}
//	}
//		protected void close(){
//			if(connection != null){
//				synchronized(locker){
//					if(connection != null){
//						if(connection != null && System.currentTimeMillis() - lastUsed > 5000){
//							flushThread.stopLoop();
//							flushThread = null;
//							try{
//								connection.close();
//							}catch(IOException e){
//								e.printStackTrace();
//								throw new RuntimeException(e);
//							}
//							connection = null;
//						}
//					}
//				}
//			}		
//		}
//		
//	
}