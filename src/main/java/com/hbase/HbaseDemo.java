package com.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020/8/17.
 *
 * @author chouFy
 */
public class HbaseDemo  extends HbaseBase{



    @Test
    public void createTable() throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //创建表描述器器 //设置列列族描述器器
        HTableDescriptor teacher = new HTableDescriptor(TableName.valueOf("friendship"));
        //执⾏行行创建操作
        teacher.addFamily(new HColumnDescriptor("friends"));
        admin.createTable(teacher); System.out.println("teacher表创建成功!!");
    }


    @Test
    public void insert() throws Exception {

//        //设定rowkey
        Put put = new Put(Bytes.toBytes("uid1"));
        //列列族，列列，value
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid2"), Bytes.toBytes("uid2"));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid5"), Bytes.toBytes("uid5"));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid7"), Bytes.toBytes("uid7"));
        //执⾏行行插⼊入
        table.put(put);

        //设定rowkey
        Put put2 = new Put(Bytes.toBytes("uid2"));
        //列列族，列列，value
        put2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"), Bytes.toBytes("uid1"));

        put2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid3"), Bytes.toBytes("uid3"));

        //执⾏行行插⼊入
        table.put(put2);

    }


    @Test
    public void delete() throws Exception {
        //准备delete对象
//        final Delete delete = new Delete(Bytes.toBytes("uid1"));
//        delete.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid2"));
//        //执⾏行行删除
//        table.delete(delete);

        final Delete delete2 = new Delete(Bytes.toBytes("uid2"));
        delete2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"));
        table.delete(delete2);

        System.out.println("删除数据成功!!");
    }


}
