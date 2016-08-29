package kafka_sparkstreaming_hbase_hzhutianqi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import kafka_sparkstreaming_hbase_hzhutianqi.CounterMap.Counter;
 
/**
 * 
 * @author hzhutianqi
 * HBase的配置实例
 *
 */
public class HBaseManagerMain {
    private static final Log LOG = LogFactory.getLog(HBaseManagerMain.class);
    // 在Eclipse中运行时报错如下
    //     Caused by: java.lang.ClassNotFoundException: org.apache.htrace.Trace
    //     Caused by: java.lang.NoClassDefFoundError: io/netty/channel/ChannelHandler
    // 需要把单独的htrace-core-3.1.0-incubating.jar和netty-all-4.0.5.final.jar导入项目中
       
    private static final String COLUMN_FAMILY_NAME = "cf";
     
    Connection connection = null;
    Admin admin = null;
    /**
     * @param args
     */
    public HBaseManagerMain(Connection connection , Admin admin){
    	this.connection = connection;
    	this.admin = admin;
    }
    
    
    
//    
//    
//    public static void main(String[] args) {
//        Configuration config = HBaseConfiguration.create();
//        config.set("hbase.zookeeper.quorum", "10.240.84.15");
//        config.set("hbase.zookeeper.property.clientPort", "2182");
//        Connection connection = null;
//        Admin admin = null;
//    
//        HBaseManagerMain manageMain = new HBaseManagerMain();
//
//        try {
//            /**
//             * HTable类读写时是非线程安全的，已经标记为Deprecated
//             * 建议通过org.apache.hadoop.hbase.client.Connection来获取实例
//             */     	
//            connection = ConnectionFactory.createConnection(config);
//            admin = connection.getAdmin();
//            
//            /**
//             *  列出所有的表
//             */
////            manageMain.listTables();
////            /**
////             * 判断表m_domain是否存在
////             */
////            boolean exists = manageMain.isExists();
////             
////            /**
////             * 存在就删除
////             */
////            if (exists) {
////               manageMain.deleteTable(admin);
////            } 
////             
////            /**
////             * 创建表
////             */
////            manageMain.createTable();
//             
////            /**
////             *  再次列出所有的表
////             */
////            manageMain.listTables();
////             
////            /**
////             * 添加数据
////             */
////            manageMain.putDatas(connection);
////             
//            /**
//             * 检索数据-表扫描
//             */
//            manageMain.scanTable(connection);
//             
//            /**
//             * 检索数据-单行读
//             */
//            manageMain.getData(connection);
//             
//            /**
//             * 检索数据-根据条件
//             */
//            manageMain.queryByFilter(connection);
//             
//            /**
//             * 删除数据
//             */
//            manageMain.deleteDatas(connection);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//         
//    }
// 
    /**
     * 列出表
     * @param admin
     * @throws IOException 
     */
    public void listTables () throws IOException {
        TableName [] names = admin.listTableNames();
        for (TableName tableName : names) {
 //           LOG.info("Table Name is : " + tableName.getNameAsString());
            System.out.println("Table Name is : " + tableName.getNameAsString());
        }
    }
     
    /**
     * 判断表是否存在
     * @param admin
     * @return
     * @throws IOException
     */
    public boolean isExists (String tablename) throws IOException {
        /**
         * org.apache.hadoop.hbase.TableName为为代表了表名字的Immutable POJO class对象,
         * 形式为<table namespace>:<table qualifier>。
         *   static TableName  valueOf(byte[] fullName) 
         *  static TableName valueOf(byte[] namespace, byte[] qualifier) 
         *  static TableName valueOf(ByteBuffer namespace, ByteBuffer qualifier) 
         *  static TableName valueOf(String name) 
         *  static TableName valueOf(String namespaceAsString, String qualifierAsString) 
         * HBase系统默认定义了两个缺省的namespace
         *     hbase：系统内建表，包括namespace和meta表
         *     default：用户建表时未指定namespace的表都创建在此
         * 在HBase中，namespace命名空间指对一组表的逻辑分组，类似RDBMS中的database，方便对表在业务上划分。
         * 
        */ 
 //       TableName tableName = TableName.valueOf(TABLE_NAME);
         
    	TableName tableName = TableName.valueOf(tablename);
        boolean exists = admin.tableExists(tableName);
        if (exists) {
 //           LOG.info("Table " + tableName.getNameAsString() + " already exists.");
            System.out.println("Table " + tableName.getNameAsString() + " already exists.");
        } else {
//            LOG.info("Table " + tableName.getNameAsString() + " not exists.");
            System.out.println("Table " + tableName.getNameAsString() + " not exists.");
        }
        return exists;
    }
     
    /**
     * 创建表
     * @param admin
     * @throws IOException
     */
    public  void createTable (String tablename) throws IOException {
        TableName tableName = TableName.valueOf( tablename);
        LOG.info("To create table named " +  tablename);
        System.out.println("To create table named " +  tablename);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(COLUMN_FAMILY_NAME);
        tableDesc.addFamily(columnDesc);
         
        admin.createTable(tableDesc);
        
        
//        if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
//            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
//            hbaseTable.addFamily(new HColumnDescriptor("name"));
//            hbaseTable.addFamily(new HColumnDescriptor("contact_info"));
//            hbaseTable.addFamily(new HColumnDescriptor("personal_info"));
//            admin.createTable(hbaseTable);
//        }
    }
     
    /**
     * 删除表
     * @param admin
     * @throws IOException
     */
    public void deleteTable (String tablename) throws IOException {
    	TableName tableName = TableName.valueOf(tablename);
 //   	LOG.info("disable and then delete table named " + tablename);
    	System.out.println("disable and then delete table named " + tablename);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
     
    /**
     * 添加数据
     * @param connection
     * @throws IOException
     */
    public void putDatas (String tableName , String columnFamily , HashMap<String , CounterMap> rowKeyCounterMap ) throws IOException {
    	TableName tablename = TableName.valueOf(tableName);
		byte[] family = Bytes.toBytes(columnFamily);
		Table table = connection.getTable(tablename);
		
		for (Entry<String, CounterMap> entry : rowKeyCounterMap.entrySet()) {  
	          CounterMap pastCounterMap = entry.getValue();  
	          rowKeyCounterMap.put(entry.getKey(), new CounterMap());  	
	          
	          byte[] rowkey = Bytes.toBytes(entry.getKey());
	          Put put = new Put(rowkey);
	        		          
	          boolean hasColumns = false;  
	          for (Entry<String, Counter> entry2 : pastCounterMap.entrySet()) {  			        	  
	        	  byte [] qualifier = Bytes.toBytes(entry2.getKey());
	        	  byte[] value = Bytes.toBytes(Long.toString(entry2.getValue().value));
	        	  put.addColumn(family , qualifier , value);		
	        	  
	            hasColumns = true;  
	          }  
	          if (hasColumns) {  
		         table.put(put);
	          }  
	        }  
        table.close();
    }
     
    /**
     * 检索数据-单行获取
     * @param connection
     * @throws IOException 
     */
    public void getData(String tableName) throws IOException {
//        LOG.info("Get data from table " + TABLE_NAME + " by family.");
        System.out.println("Get data from table " + tableName + " by family.");
        TableName tablename = TableName.valueOf(tableName);
        byte [] family = Bytes.toBytes(COLUMN_FAMILY_NAME);
        byte [] row = Bytes.toBytes("Counter");
        Table table = connection.getTable(tablename);
         
        Get get = new Get(row);
        get.addFamily(family);
        // 也可以通过addFamily或addColumn来限定查询的数据
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
            // @Deprecated
            // LOG.info(cell.getQualifier() + "\t" + cell.getValue());
//            LOG.info(qualifier + "\t" + value);
            System.out.println(qualifier + "\t" + value);
        }
         
    }
     
    /**
     * 检索数据-表扫描
     * @param connection
     * @throws IOException 
     */
    public void scanTable(String tableName) throws IOException {
 //       LOG.info("Scan table " + TABLE_NAME + " to browse all datas.");
        System.out.println("Scan table " + tableName + " to browse all datas.");
        TableName tablename = TableName.valueOf(tableName);
        byte [] family = Bytes.toBytes(COLUMN_FAMILY_NAME);
         
        Scan scan = new Scan();
        scan.addFamily(family);
         
        Table table = connection.getTable(tablename);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Iterator<Result> it = resultScanner.iterator(); it.hasNext(); ) {
            Result result = it.next();
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                // @Deprecated
                // LOG.info(cell.getQualifier() + "\t" + cell.getValue());
//                LOG.info(qualifier + "\t" + value);
                System.out.println(qualifier + "\t" + value);
            }
        }
    }
 
    /**
     * 安装条件检索数据
     * @param connection
     */
    private void queryByFilter(Connection connection) {
        // 简单分页过滤器示例程序
        Filter filter = new PageFilter(15);     // 每页15条数据
        int totalRows = 0;
        byte [] lastRow = null;
         
        Scan scan = new Scan();
        scan.setFilter(filter);
         
        // 略
    }
     
    /**
     * 删除数据
     * @param connection
     * @throws IOException 
     */
//    private void deleteDatas(Connection connection) throws IOException {
// //       LOG.info("delete data from table " + TABLE_NAME + " .");
//        System.out.println("delete data from table " + TABLE_NAME + " .");
//        TableName tableName = TableName.valueOf(TABLE_NAME);
//        byte [] family = Bytes.toBytes(COLUMN_FAMILY_NAME);
//        byte [] row = Bytes.toBytes("baidu.com_19991011_20151011");
//        Delete delete = new Delete(row);
//         
//        // @deprecated Since hbase-1.0.0. Use {@link #addColumn(byte[], byte[])}
//        // delete.deleteColumn(family, qualifier);            // 删除某个列的某个版本
//        delete.addColumn(family, Bytes.toBytes("owner"));
//         
//        // @deprecated Since hbase-1.0.0. Use {@link #addColumns(byte[], byte[])}
//        // delete.deleteColumns(family, qualifier)            // 删除某个列的所有版本
//         
//        // @deprecated Since 1.0.0. Use {@link #(byte[])}
//        // delete.addFamily(family);                           // 删除某个列族
//         
//        Table table = connection.getTable(tableName);
//        table.delete(delete);
//    }
}