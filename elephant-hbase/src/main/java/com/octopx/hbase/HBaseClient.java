package com.octopx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yuyang on 16/11/23.
 */
public class HBaseClient {
    private Configuration conf;
    private Connection conn;
    private Admin admin;

    public HBaseClient() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", "192.168.200.138:60000");
        conf.set("hbase.zookeeper.quorum", "192.168.200.138:2181,192.168.200.139:2181,192.168.200.140:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HBaseClient base = new HBaseClient();
        base.listTables();  //列出所有的表
        HTableDescriptor table = base.createTable("tes");    //新建HBase表
        base.listTables();  //列出所有的表
        //base.deleteTable(table.getTableName());    //删除新建的表
        //base.listTables();

        Put put = new Put(Bytes.toBytes("row3"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"), Bytes.toBytes("value12"));
        base.addRow(table.getTableName(), put);

        Get get = new Get(Bytes.toBytes("row3"));

        base.scanTable(table.getTableName());
    }

    /**
     * 列出所有的表
     */
    public void listTables() {
        try {
            HTableDescriptor[] tables = admin.listTables();
            for (HTableDescriptor table : tables) {
                System.out.println(table.getTableName().getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 新建表
     * @param name
     */
    public HTableDescriptor createTable(String name) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(name));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
        tableDescriptor.addFamily(columnDescriptor);
        try {
            if (!admin.tableExists(tableDescriptor.getTableName())) {
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableDescriptor;
    }

    /**
     * 删除表
     * @param tableName
     */
    public void deleteTable(TableName tableName) {
        try {
            if (!admin.isTableDisabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 向表里添加数据
     */
    public void addRow(TableName tableName, Put put) {
        try {
            Table table = conn.getTable(tableName);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 遍历表中的数据
     */
    public void scanTable(TableName tableName) {
        try {
            Table table = conn.getTable(tableName);
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
            Filter filter = new PrefixFilter(Bytes.toBytes("row"));
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            Result result = null;
            while ((result = scanner.next()) != null) {
                for (Cell cell : result.listCells()) {
                    System.out.println(CellUtil.getCellKeyAsString(cell));
                }
                byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("b"));
                System.out.println(new String(value));
            }

            Delete delete = new Delete(Bytes.toBytes("row1"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
