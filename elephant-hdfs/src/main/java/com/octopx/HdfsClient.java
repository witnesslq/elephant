package com.octopx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

public class HdfsClient {
    private static Configuration conf;
	private FileSystem fs;

    static {
        conf = new Configuration();

        /**
         * 这个配置文件主要是记录Kerberos的相关配置
         */
        System.setProperty("java.security.krb5.conf", "/Users/yuyang/Desktop/krb5.conf");

        conf.set("hadoop.security.authentication", "Kerberos");
        //设置用户Principal
        conf.set("kerberos.principal", "yuy@DEV.DXY.CN");
        //设置用户Keytab
        conf.set("keytab.file", "/Users/yuyang/Desktop/yuy.keytab");

        UserGroupInformation.setConfiguration(conf);

        try {
            UserGroupInformation.loginUserFromKeytab(conf.get("kerberos.principal"), conf.get("keytab.file"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HdfsClient() throws Exception {
        conf.set("fs.defaultFS", "hdfs://192.168.200.138:8020/");

        //提供配置文件
        //conf.addResource(new Path("/u/hadoop-1.0.2/conf/core-site.xml"));
        //conf.addResource(new Path("/u/hadoop-1.0.2/conf/hdfs-site.xml"));

        fs = FileSystem.get(conf);
    }

    public void list(Path path) throws IOException {
        FileStatus files[] = fs.listStatus(path);

        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }

    public static void main(String[] args) {
        HdfsClient client = null;
        try {
            client = new HdfsClient();
            URI uri = URI.create("/tmp");
            Path dest = new Path(uri);
            client.list(dest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
