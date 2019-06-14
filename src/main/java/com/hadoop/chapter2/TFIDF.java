package com.hadoop.chapter2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
 * Created with IDEA by ChouFy on 2019/3/6.
 *
 * @author Zhoufy
 */
public class TFIDF extends Configured implements Tool {

    private static class TfidfMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] texts = line.split(" ");
            for (String text : texts) {
                context.write(new Text(text), new IntWritable(1));
            }
        }
    }

    private static class TfidfReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        Double docCount = 508d;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : values) {
                sum += count.get();
            }
            //  idf 等于 log( 文章的总数量 / 包含该次的文档)  +1 防止分母出现0
            double idf = Math.log(docCount / (sum + 1.0));
            context.write(key, new DoubleWritable(idf));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        if (strings.length != 2) {
            return 0;
        }

        String inPath = strings[0];
        String outPath = strings[1];

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(new Path(outPath));
        fs.close();

        Job job = Job.getInstance(conf, "tfidf");

        job.setJarByClass(TFIDF.class);

        job.setMapperClass(TfidfMapper.class);
        job.setReducerClass(TfidfReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Double.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);

        return job.isSuccessful() ? 1 : 0;
    }


    public static void main(String[] args) throws Exception {

        // 记录开始时间
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();

        // 运行任务
        int res = ToolRunner.run(new Configuration(), new TFIDF(), args);
        // 输出任务耗时
        Date end = new Date();
        float time = (float) ((end.getTime() - start.getTime()) / 1000.0);
        System.out.println("任务开始：" + formatter.format(start));
        System.out.println("任务结束：" + formatter.format(end));
        System.out.println("任务耗时：" + String.valueOf(time) + " 秒");
        System.exit(res);
    }
}
