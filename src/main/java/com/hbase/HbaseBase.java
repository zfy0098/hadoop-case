package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/6/10.
 *
 * @author Zhoufy
 */
public class HbaseBase {
    protected static Configuration conf = HBaseConfiguration.create();

    protected static Connection connection = null;

    protected static String tableName = "app_role";

    protected static Table table = null;


    static  {
        try {
            conf.set("hbase.zookeeper.quorum", "master,slaves1,slaves2");
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
