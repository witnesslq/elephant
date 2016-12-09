package com.octopx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Created by yuyang on 16/12/8.
 */
public class KerberizedHBaseClient {
    private static Configuration conf = null;
    private static Connection conn = null;
    private static Admin admin = null;

    static {
        /**
         * 这个配置文件主要是记录Kerberos的相关配置
         */
        System.setProperty("java.security.krb5.conf", "/Users/yuyang/Desktop/krb5.conf");

        conf = HBaseConfiguration.create();
        conf.set("hbase.master", "192.168.200.138:60000");
        conf.set("hadoop.security.authentication", "Kerberos");
        //设置密码相关
        conf.set("keytab.file", "/Users/yuyang/Desktop/yuy.keytab");
        //设置用户Principal
        conf.set("kerberos.principal", "yuy@DEV.DXY.CN");
        //设置ZooKeeper
        conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.200.138:2181,192.168.200.139:2181,192.168.200.140:2181");

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("yuy@DEV.DXY.CN", "/Users/yuyang/Desktop/yuy.keytab");
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

    public static void main(String[] args) {
        KerberizedHBaseClient client = new KerberizedHBaseClient();
        client.listTables();
    }
}
