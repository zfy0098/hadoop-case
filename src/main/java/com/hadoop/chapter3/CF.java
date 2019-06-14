package com.hadoop.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IDEA by ChouFy on 2019/3/4.
 *
 * @author Zhoufy
 */
public class CF {


    public static class CfMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            String[] ss = text.split("\t");
            if (ss.length != 3) {
                return;
            }
            StringBuffer sbf = new StringBuffer();
            sbf.append(ss[0]).append("\t").append(ss[2]);
            context.write(new Text(ss[1]), new Text(sbf.toString()));
        }
    }


    public static class CfReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> userScoreList = new ArrayList<>();
            Iterator<Text> it = values.iterator();
            String item = key.toString();
            double num = 0.0;
            while (it.hasNext()) {
                String line = it.next().toString();
                String[] value = line.split("\t");

                String user = value[0];
                String score = value[1];
                userScoreList.add(user + "\t" + score);

            }
            for (String t : userScoreList) {
                String[] tuple = t.split("\t");
                num += Math.pow(Double.parseDouble(tuple[1]), 2);
            }
            num = Math.sqrt(num);

            for (String t : userScoreList) {
                context.write(new Text(t.split("\t")[0]), new Text(item + "\t" + Double.parseDouble(t.split("\t")[1]) / num));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String inPath;
        String outPath;
        if (args.length != 2) {
            return;
        }
        inPath = args[0];
        outPath = args[1];

        Configuration conf = new Configuration();


        Path path = new Path(outPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.deleteOnExit(path);
        }
        fs.close();

        Job job = Job.getInstance(conf, "cf");

        job.setJarByClass(CF.class);
        job.setMapperClass(CfMapper.class);
        job.setReducerClass(CfReduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);

    }
}
