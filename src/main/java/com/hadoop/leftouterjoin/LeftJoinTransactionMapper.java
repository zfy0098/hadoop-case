package com.hadoop.leftouterjoin;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * Created with IDEA by ChouFy on 2019/6/18.
 *
 * @author Zhoufy
 */
public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();

    /**
     * @param key: system generated, ignored here
     * @param value: <transaction_id><TAB><product_id><TAB><user_id><TAB><quantity><TAB><amount>
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), ",");
        String productID = tokens[1];
        String userID = tokens[2];
        // make sure products arrive at a reducer after location
        outputKey.set(userID, "2");
        outputValue.set("P", productID);
        context.write(outputKey, outputValue);
    }

}
