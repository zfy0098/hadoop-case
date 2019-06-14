package com.hbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created with IDEA by ChouFy on 2019/4/30.
 *
 * @author Zhoufy
 */
public class HBaseTest extends HbaseBase {


    private void scan() throws Exception {
        System.out.println("Hello Hbase");

        Scan scan = new Scan();
        scan.setCaching(100);
        scan.addFamily("mete_data".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("S6.阿特流姆")));
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);


        for (Result result : scanner) {
            byte[] bytes = result.getRow();
            System.out.println("====" + new String(bytes));

            List<Cell> list = result.listCells();
            for (Cell cell : list) {
                System.out.print(new String(CellUtil.cloneFamily(cell)));
                System.out.print(":");
                System.out.print(new String(CellUtil.cloneQualifier(cell)));
                System.out.print(" ");
                System.out.print(new String(CellUtil.cloneValue(cell)));
                System.out.println("   " + cell.getTimestamp());
            }
        }

        scanner.close();
        table.close();
    }


    private void put() throws Exception {


        /**
         * {"roleLevel":"28","imei":"99001008611343","api_ver":"1.4.1","app_ver":"1.1.0","partyRoleID":"0","taskName":"",
         * "roleCTime":"1556726160","roleLevelIMTime":"0","app_ver_code":"110","format":"json","t":1556726398,"professionID":"0","serverID":"6",
         * "partyRoleName":"无","channel_id":"205","device_os_ver":"Android 8.1.0","serverName":"S6红心皇后","ic":"track","source":"205",
         * "sign":"5646fc64709525bc1430aaa208f1b57c","roleName":"碧轮的微笑","roleID":"6069079","friendList":"无","package_id":"3250",
         * "partyName":"null","taskId":"","profession":"无","appid":"10090","acid":"329","honorName":"","device_name":"Mi Note 3",
         * "taskStatus":"","sdk_ver":"1.4.1","child_id":"89","moneyNum":"781","os":"1","power":"21949","partyID":"0","honorId":"",
         * "vip":"0","gender":"无","cat":"online","uToken":"1122333062936662017"}
         */
        List<String> lines = Files.readAllLines(Paths.get("/Users/zhoufy/Desktop/online_2019-05-01.log"));

        for (String line : lines) {
            JSONObject json = JSONObject.parseObject(line);

            String uid = json.getString("uToken");
            Integer appid = json.getInteger("appid");
            Integer childID = json.getInteger("child_id");
            Integer channelID = json.getInteger("channel_id");
            Integer acid = json.getInteger("acid");
            String aoleName = json.getString("roleName") == null ? "" : json.getString("roleName");
            String roleLevel = json.getString("roleLevel") == null ? "" : json.getString("roleLevel");
            String serverName = json.getString("serverName") == null ? "" : json.getString("serverName");

            Put put = new Put(Bytes.toBytes(uid));
            put.addColumn(Bytes.toBytes("mete_data"), Bytes.toBytes("appid"), Bytes.toBytes(appid));
            put.addColumn(Bytes.toBytes("mete_data"), Bytes.toBytes("childID"), Bytes.toBytes(childID));
            put.addColumn(Bytes.toBytes("mete_data"), Bytes.toBytes("channelID"), Bytes.toBytes(channelID));
            put.addColumn(Bytes.toBytes("mete_data"), Bytes.toBytes("acid"), Bytes.toBytes(acid));
            put.addColumn(Bytes.toBytes("mete_data"), Bytes.toBytes("aoleName"), Bytes.toBytes(aoleName));
            put.addColumn(Bytes.toBytes("mete_data"), Bytes.toBytes("roleLevel"), Bytes.toBytes(roleLevel));
            put.addColumn(Bytes.toBytes("mete_data"), Bytes.toBytes("serverName"), Bytes.toBytes(serverName));

            table.put(put);
            System.out.println("insert record " + uid + " to table " + tableName + " success");
        }
    }


    private void del() throws Exception {
        Delete del = new Delete(Bytes.toBytes("1001"));
        table.delete(del);
    }

    public static void main(String[] args) throws Exception {

        HBaseTest hBaseTest = new HBaseTest();
//        hBaseTest.put();
        hBaseTest.scan();

    }
}
