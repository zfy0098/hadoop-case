package com.hadoop.chapter3;

import com.utils.ParamsUtils;
import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2019/3/17.
 *
 * @author Zhoufy
 */
public class MapReduceInverted {



    private static class InvertedStepOneMap extends Mapper<LongWritable, Text, Text, Tuple>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] ss = line.split("\t");
            if(ss.length != 3){
                return;
            }

            String user = ss[0];
            String item = ss[1];
            String score = ss[2];

            Schema schema = new Schema();
            schema.addField("user", String.class);
            schema.addField("score", String.class);
            Tuple tuple = schema.instantiate();

            tuple.setSymbol("user", user);
            tuple.setSymbol("score", score);
            context.write(new Text(item), tuple);
        }
    }


    private  static class InvertedStepOneReducer extends Reducer<Text, Tuple, Text, Tuple>{
        @Override
        protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {

            double sum = 0.0;

            List<Map<String,Double>> list = new ArrayList<>();

            for (Tuple tuple : values){
                String score = tuple.getSymbol("score");
                String user = tuple.getSymbol("user");
                sum += Math.pow(Float.parseFloat(score), 2);

                Map<String, Double> map = new HashMap<>();
                map.put(user, sum);
                list.add(map);
            }

            for (Map<String,Double> map : list){
                for (Map.Entry<String,Double> entry : map.entrySet()){
                    String user = entry.getKey();
                    Double score = entry.getValue();

                    Schema schema = new Schema();
                    schema.addField("items", String.class);
                    schema.addField("score", Double.class);
                    Tuple tuple = schema.instantiate();

                    tuple.set("items", key.toString());
                    tuple.set("score", score / sum);

                    context.write(new Text(user), tuple);
                }
            }
        }
    }


    private static class InvertedStepTwoMap extends Mapper<LongWritable ,Text, Text, Tuple>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }



    public static void main(String[] args) {


        args = new String[]{"/music/input/music_uis.data" , "/music/out1"};

        try {
            Job job = ParamsUtils.pretreatment(args, 2);
            job.setJobName("InvertedStepOne");

            job.setJarByClass(MapReduceInverted.class);

            job.setMapperClass(InvertedStepOneMap.class);
            job.setReducerClass(InvertedStepOneReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Tuple.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Tuple.class);

            job.setInputFormatClass(TextInputFormat.class);

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
