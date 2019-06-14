package com.hadoop.chapter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/1/29.
 *
 * @author Zhoufy
 */
public class WordCount {


    public static class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            for (int i = 0; i < content.length(); i++) {
                context.write(new Text(String.valueOf(content.charAt(i))), new LongWritable(1));
            }
        }
    }


    public static class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int l = 0;
            for (LongWritable val : values) {
                l += val.get();
            }
            context.write(key, new LongWritable(l));
        }
    }

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

////        map的压缩输出
//        configuration.setBoolean("mapred.compress.map.out", true);
//        configuration.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
//
////        reduce的压缩输出
//        configuration.setBoolean("mapred.output.compress", true);


        Job job = Job.getInstance(configuration, "wordCount");
        job.setJobName("wordCount");

        job.setJarByClass(WordCount.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);

    }
}
