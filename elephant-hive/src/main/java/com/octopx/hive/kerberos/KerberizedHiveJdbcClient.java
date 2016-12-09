package com.octopx.hive.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.*;

/**
 * Created by yuyang on 16/12/8.
 */
public class KerberizedHiveJdbcClient {
    private static String hiveServerUrl = "jdbc:hive2://192.168.200.138:10000/default;principal=hive/data138.dev.dxy.cn@DEV.DXY.CN";
    private static Connection conn = null;

    static {
        System.setProperty("java.security.krb5.conf", "/Users/yuyang/Desktop/krb5.conf");

        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("yuy@DEV.DXY.CN", "/Users/yuyang/Desktop/yuy.keytab");

            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection(hiveServerUrl);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void query(String sql) {
        try {
            Statement statement = conn.createStatement();
            ResultSet result = statement.executeQuery(sql);
            while (result.next()) {
                System.out.println(result.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        KerberizedHiveJdbcClient client = new KerberizedHiveJdbcClient();
        client.query("show databases");
    }
}
