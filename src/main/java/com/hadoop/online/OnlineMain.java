package com.hadoop.online;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class OnlineMain {


    public static void main(String[] args) throws Exception {

        String inputPath;
        String outPath;

        if (args.length == 2) {
            inputPath = args[0];
            outPath = args[1];
        } else {
            return;
        }

        Configuration configuration = new Configuration();
        Path path = new Path(outPath);
        FileSystem file = FileSystem.get(configuration);

        if (file.exists(path)) {
            file.deleteOnExit(path);
        }
        file.close();

        Job job = Job.getInstance(configuration, "onlineLong");
        job.setJobName("onlineLong");

        job.setJarByClass(OnlineMain.class);

        job.setNumReduceTasks(2);


        job.setMapperClass(OnlineMap.class);
        job.setReducerClass(OnlineReduce.class);


        job.setMapOutputKeyClass(OnlineWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(OnlineWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);
    }

}
