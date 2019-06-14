package com.hadoop.online;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class OnlineReduce extends Reducer<OnlineWritable, Text, OnlineWritable, LongWritable> {


    @Override
    protected void reduce(OnlineWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int startTime = 0;
        int lastTime = 0;
        long onlineLong = 0;
        int threshold = 60;

        List<Integer> list = new ArrayList<>();

        for (Text val : values) {
            String text = val.toString();
            JSONObject json = JSONObject.parseObject(text);

            list.add(json.getInteger("t"));
        }
        Collections.sort(list);

        int count = 0;
        for (int t : list) {
            if (startTime == 0) {
                startTime = t;
            }
            if (startTime < t && t - lastTime <= threshold) {
                onlineLong += (t - lastTime);
                lastTime = t;
            } else {
                lastTime = t;
                count++;
            }
        }

        if (count > 0 && onlineLong == 0) {
            if(count > 1){
                count = count-1;
            }
            onlineLong += count * threshold;
        }
        context.write(key, new LongWritable(onlineLong));


    }


}
