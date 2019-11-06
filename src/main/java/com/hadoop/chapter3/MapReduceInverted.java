package com.hadoop.chapter3;

import com.utils.ParamsUtils;
import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.math.BigDecimal;
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


    private static class InvertedStepOneMap extends Mapper<LongWritable, Text, Text, Tuple> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] ss = line.split("\t");
            if (ss.length != 3) {
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


    private static class InvertedStepOneReducer extends Reducer<Text, Tuple, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {

            double sum = 0.0;

            List<Map<String, Double>> list = new ArrayList<>();

            for (Tuple tuple : values) {
                String score = tuple.getSymbol("score");
                String user = tuple.getSymbol("user");
                sum += Math.pow(Double.parseDouble(score), 2);

                Map<String, Double> map = new HashMap<>(4);
                map.put(user, Double.parseDouble(score));
                list.add(map);
            }

            sum = Math.sqrt(sum);

            for (Map<String, Double> map : list) {
                for (Map.Entry<String, Double> entry : map.entrySet()) {
                    String user = entry.getKey();
                    Double score = entry.getValue();

                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(key.toString());
                    stringBuilder.append("\t");
                    BigDecimal b1 = new BigDecimal(score);
                    BigDecimal b2 = new BigDecimal(String.valueOf(sum));
                    stringBuilder.append(b1.divide(b2, 2, 4).doubleValue());
                    context.write(new Text(user), new Text(stringBuilder.toString()));
                }
            }
        }
    }

    private static boolean stepOne(String[] args) {

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

            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    private static class InvertedStepTwoMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split("\t");

            String user = values[0];
            String item = values[1];
            String score = values[2];

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(item).append("\t").append(score);

            context.write(new Text(user), new Text(stringBuilder.toString()));
        }
    }


    private static class InvertedStepTwoReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<PairOfStrings> list = new ArrayList<>();
            for (Text text : values) {

                String item = text.toString().split("\t")[0];
                String score = text.toString().split("\t")[1];
                PairOfStrings pairOfStringFloat = new PairOfStrings();
                pairOfStringFloat.set(item, score);
                list.add(pairOfStringFloat);
            }

            for (int i = 0; i < list.size() - 1; i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    PairOfStrings pair1 = list.get(i);
                    PairOfStrings pair2 = list.get(j);

                    String itemA = pair1.getLeftElement();
                    String itemAScore = pair1.getRightElement();

                    String itemB = pair2.getLeftElement();
                    String itemBScore = pair2.getRightElement();
                    StringBuilder stringBuilder = new StringBuilder();
                    BigDecimal b1 = new BigDecimal(itemAScore);
                    BigDecimal b2 = new BigDecimal(itemBScore);
                    double s = b1.multiply(b2).doubleValue();
                    stringBuilder.append(itemA).append("\t").append(itemB).append("\t").append(s);
                    context.write(NullWritable.get(), new Text(stringBuilder.toString()));

                    stringBuilder = new StringBuilder();
                    stringBuilder.append(itemB).append("\t").append(itemA).append("\t").append(s);
                    context.write(NullWritable.get(), new Text(stringBuilder.toString()));
                }
            }
        }
    }


    private static boolean stepTwo(String[] args) {
        try {
            Job job = ParamsUtils.pretreatment(args, 2);
            job.setJobName("InvertedStepTwo");

            job.setJarByClass(MapReduceInverted.class);

            job.setMapperClass(InvertedStepTwoMap.class);
            job.setReducerClass(InvertedStepTwoReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);

            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    private static class InvertedStepThreeMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            String itemA = values[0];
            String itemB = values[1];
            String score = values[2];

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(itemA).append("\001").append(itemB);
            context.write(new Text(stringBuilder.toString()), new Text(score));
        }
    }

    private static class InvertedStepThreeReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double score = 0.0;

            for (Text text : values) {
                BigDecimal b1 = new BigDecimal(String.valueOf(score));
                BigDecimal b2 = new BigDecimal(text.toString());
                score = b1.add(b2).doubleValue();
            }

            String[] keys = key.toString().split("\001");

            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append(keys[0]).append("\t").append(keys[1]).append("\t").append(score);

            context.write(NullWritable.get(), new Text(stringBuilder.toString()));
        }
    }


    private static boolean stepThree(String[] args) {
        try {
            Job job = ParamsUtils.pretreatment(args, 2);
            job.setJobName("InvertedStepThree");

            job.setJarByClass(MapReduceInverted.class);

            job.setMapperClass(InvertedStepThreeMap.class);
            job.setReducerClass(InvertedStepThreeReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);

            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    public static void main(String[] args) {
        String inputPath = "/music/input/cftest";
        String stepOneOut = "/music/out1";
        String stepTwoOut = "/music/out2";
        String stepThreeOut = "/music/out3";

        args = new String[]{inputPath, stepOneOut};
        boolean flag = stepOne(args);
        System.out.println(flag);
        if (flag) {
            args = new String[]{stepOneOut, stepTwoOut};
            flag = stepTwo(args);
            if (flag) {
                args = new String[]{stepTwoOut, stepThreeOut};
                flag = stepThree(args);
                System.out.println(flag);
            }
        }
    }
}
