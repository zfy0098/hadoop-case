package com.hadoop.topk;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created with IDEA by ChouFy on 2019/7/9.
 *
 * @author Zhoufy
 */
public class TopKAapp {


    public static class MyMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {
        public static final int K = 100;
        private TreeMap<Long, Long> tree = new TreeMap<>();

        @Override
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            long temp = Long.parseLong(text.toString());
            tree.put(temp, temp);
            if (tree.size() > K) {
                tree.remove(tree.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Long text : tree.values()) {
                context.write(NullWritable.get(), new LongWritable(text));
            }
        }
    }

    public static class MyReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
        public static final int K = 100;
        private TreeMap<Long, Long> tree = new TreeMap<>();

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Long val : tree.descendingKeySet()) {
                context.write(NullWritable.get(), new LongWritable(val));
            }
        }

        @Override
        protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                tree.put(value.get(), value.get());
                if (tree.size() > K) {
                    tree.remove(tree.firstKey());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {


        if(args.length < 2){
            throw new Exception("params length error");
        }

        String inputPath = args[0];
        String outPath = args[1];


        Configuration configuration = new Configuration();
        Path path = new Path(outPath);
        FileSystem file = FileSystem.get(configuration);

        if (file.exists(path)) {
            file.deleteOnExit(path);
        }
        file.close();

        Job job = Job.getInstance(configuration, "topK");
        job.setJobName("topK");

        FileInputFormat.setInputPaths(job, new Path(inputPath));

        job.setJarByClass(TopKAapp.class);



        job.setMapperClass(TopKAapp.MyMapper.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(TopKAapp.MyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);


        FileOutputFormat.setOutputPath(job, new Path(outPath));


        job.waitForCompletion(true);

    }

}
