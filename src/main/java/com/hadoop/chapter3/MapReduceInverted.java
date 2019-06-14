package com.hadoop.chapter3;

import com.huaban.analysis.jieba.JiebaSegmenter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/3/17.
 *
 * @author Zhoufy
 */
public class MapReduceInverted {



    private static class InvertedMap extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] ss = line.split("\t");
            if(ss.length != 2){
                return;
            }
            String musicID = ss[0];
            String musicName = ss[1];
//            JiebaSegmenter


        }
    }




    public static void main(String[] args) {

    }
}
