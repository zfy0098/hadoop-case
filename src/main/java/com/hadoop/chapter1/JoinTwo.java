package com.hadoop.chapter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class JoinTwo extends Configured implements Tool {



    private static class JoinMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {




            String line = value.toString();
            String[] ss = line.split("\t");
            if (ss.length != 2) {
                return;
            }
            context.write(new Text(ss[0]), new Text(ss[1]));
        }
    }


    private static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            StringBuffer sbf = new StringBuffer();
            for (Text text : values) {
                sbf.append(text.toString()).append("\t");
            }
            context.write(key, new Text(sbf.toString()));
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
        fs.deleteOnExit(new Path(outPath));
        fs.close();

        Job job = Job.getInstance(conf);
        job.setJobName("join");

        job.setJarByClass(JoinTwo.class);

        job.setMapperClass(JoinMap.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);


        return 0;
    }


    public static void main(String[] args) throws Exception {
        // 记录开始时间
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();

        // 运行任务
        int res = ToolRunner.run(new Configuration(), new JoinTwo(), args);
        // 输出任务耗时
        Date end = new Date();
        float time = (float) ((end.getTime() - start.getTime()) / 1000.0);
        System.out.println("任务开始：" + formatter.format(start));
        System.out.println("任务结束：" + formatter.format(end));
        System.out.println("任务耗时：" + String.valueOf(time) + " 秒");
        System.exit(res);
    }
}
