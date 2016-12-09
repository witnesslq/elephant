package com.octopx.hive.base;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * Created by yuyang on 16/11/24.
 */
public class BaseHiveMetastoreClient {
    private HiveConf conf;
    private HiveMetaStoreClient client;

    public BaseHiveMetastoreClient() {
        conf = new HiveConf();
        conf.set("hive.metastore.url", "192.168.200.138");
        conf.set("hive.metastore.port", "9083");
        try {
            client = new HiveMetaStoreClient(conf);
        } catch (MetaException e) {
            e.printStackTrace();
        }
    }

    public void listDatabases() {
        try {
            for (String db : client.getAllDatabases()) {
                System.out.println(db);
            }
        } catch (MetaException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        BaseHiveMetastoreClient baseHiveMetastore = new BaseHiveMetastoreClient();
        baseHiveMetastore.listDatabases();
    }
}
