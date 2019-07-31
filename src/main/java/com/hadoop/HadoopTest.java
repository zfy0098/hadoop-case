package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/7/24.
 *
 * @author Zhoufy
 */
public class HadoopTest {

    public static class HadoopTestMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        if (args.length != 2) {
            throw new Exception("params length error");
        }

        String inputPath = args[0];
        String outPath = args[1];

        Path path = new Path(outPath);
        if (fs.exists(path)) {
            fs.deleteOnExit(path);
        }
        fs.close();

        Job job = Job.getInstance(configuration);

        job.setJobName("hadoop test");
        job.setMapperClass(HadoopTestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setJarByClass(HadoopTest.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, path);

        job.waitForCompletion(true);
    }
}
