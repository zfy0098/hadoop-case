package com.hadoop.leftouterjoin;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/6/19.
 *
 * @author Zhoufy
 */
public class LocationCountMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}
