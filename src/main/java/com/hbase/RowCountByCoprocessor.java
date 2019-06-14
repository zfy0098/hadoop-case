package com.hbase;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created with IDEA by ChouFy on 2019/6/11.
 *
 * @author Zhoufy
 */
public class RowCountByCoprocessor extends HbaseBase {

    public void rowCountByCoprocessor(){
        try {
            //提前创建connection和conf
            Admin admin = connection.getAdmin();
            TableName name=TableName.valueOf(tableName);
            //先disable表，添加协处理器后再enable表
            admin.disableTable(name);
            HTableDescriptor descriptor = admin.getTableDescriptor(name);
            String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
            if (! descriptor.hasCoprocessor(coprocessorClass)) {
                descriptor.addCoprocessor(coprocessorClass);
            }
            admin.modifyTable(name, descriptor);
            admin.enableTable(name);

            //计时
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            Scan scan = new Scan();
            FilterList filterList = new FilterList();
//            filterList.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("S6.阿特流姆")));
            Filter f = new SingleColumnValueFilter(Bytes.toBytes("mete_data"), Bytes.toBytes("appid"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(10090)));
            filterList.addFilter(f);
            scan.setFilter(filterList);
            AggregationClient aggregationClient = new AggregationClient(conf);

            System.out.println("RowCount: " + aggregationClient.rowCount(name, new LongColumnInterpreter(), scan));
            stopWatch.stop();
            System.out.println("统计耗时：" +stopWatch.getTime());
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        RowCountByCoprocessor rowCountByCoprocessor = new RowCountByCoprocessor();
        rowCountByCoprocessor.rowCountByCoprocessor();
    }

}
