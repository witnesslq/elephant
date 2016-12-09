package com.octopx.hive.thrift;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by yuyang on 16/11/16.
 */
public class ThriftMetastoreClient {

    public static void main(String[] args) {
        final TSocket transport = new TSocket("192.168.200.138", 9083, 10000);
        final TProtocol protocol = new TBinaryProtocol(transport);
        final ThriftHiveMetastore.Client client =  new ThriftHiveMetastore.Client(protocol);

        try {
            transport.open();
            System.out.println("open success");
            for (String db : client.get_all_databases()) {
                System.out.println(db);
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
