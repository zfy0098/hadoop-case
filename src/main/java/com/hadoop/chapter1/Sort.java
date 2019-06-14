package com.hadoop.chapter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IDEA by ChouFy on 2019/1/29.
 *
 * @author Zhoufy
 */
public class Sort extends Configured implements Tool {


    public static class SortMap extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 读取源数据
            String line = value.toString();
            context.write(new IntWritable(Integer.parseInt(line.split("\t")[0])), new Text(line.split("\t")[1]));
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static class SortPartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable intWritable, Text text, int number) {
            int keyInt = Integer.parseInt(intWritable.toString());
            int thresholdValue = 50;
            if (keyInt < thresholdValue) {
                return 0;
            } else {
                return 1;
            }
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        String inputPath;
        String outPath;

        if (strings.length != 2) {
            return 1;
        } else {
            inputPath = strings[0];
            outPath = strings[1];
        }

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outPath);

        System.out.println(fs.exists(path));

        boolean isOk = fs.deleteOnExit(path);
        if (isOk) {
            System.out.println("delete ok!");
        } else {
            System.out.println("delete failure");
        }
        fs.close();


        Job job = Job.getInstance(conf, "Word count");
        job.setJarByClass(Sort.class);
        job.setJobName("Word count test");

        job.setNumReduceTasks(2);
        job.setPartitionerClass(SortPartitioner.class);



        job.setMapperClass(SortMap.class);

        // combiner use the same method as reducer and calculate the local count sum
        job.setCombinerClass(SortReducer.class);

        // reducer task get the total count of each word
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // set the output KEY and VALUE type
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // use the arguments to set the job input and output path
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }


    /**
     * @param args
     * @throws Exception one arguments is the input path
     *                   the other arguments is the output path
     */
    public static void main(String[] args) throws Exception {

        // 记录开始时间
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();

        // 运行任务
        int res = ToolRunner.run(new Configuration(), new Sort(), args);
        // 输出任务耗时
        Date end = new Date();
        float time = (float) ((end.getTime() - start.getTime()) / 1000.0);
        System.out.println("任务开始：" + formatter.format(start));
        System.out.println("任务结束：" + formatter.format(end));
        System.out.println("任务耗时：" + String.valueOf(time) + " 秒");
        System.exit(res);
    }
}
