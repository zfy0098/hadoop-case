package com.hadoop.leftouterjoin;


import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created with IDEA by ChouFy on 2019/6/19.
 *
 * @author Zhoufy
 */
public class LocationCountReducer  extends Reducer<Text, Text, Text, LongWritable> {

    @Override
    public void reduce(Text productID, Iterable<Text> locations, Context context)
            throws  IOException, InterruptedException {
        //
        Set<String> set = new HashSet<>();
        //
        for (Text location: locations) {
            set.add(location.toString());
        }
        //
        context.write(productID, new LongWritable(set.size()));
    }
}
