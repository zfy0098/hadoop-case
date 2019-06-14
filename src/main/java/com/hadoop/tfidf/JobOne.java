package com.hadoop.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IDEA by ChouFy on 2019/2/19.
 *
 * @author Zhoufy
 */
public class JobOne extends Configured implements Tool {


    public static class JobOneMapper extends Mapper<Text, Text, Text , IntWritable>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();

            String[] content = text.split("\t");

            if(content.length != 2){
                return ;
            }
            // 分词器
            StringReader sr =new StringReader(content[1]);
            IKSegmenter iks = new IKSegmenter(sr, true);
            Lexeme word = null;
            while ((word = iks.next()) != null) {
                // 获取分词结果的单词
                String w= word.getLexemeText();
                // 词语_id   1
                context.write(new Text(key+"_"+content[0]), new IntWritable(1));
            }
            // |D|  count  1
            context.write(new Text("count"), new IntWritable(1));
        }
    }


    public static class JobOneReduce extends Reducer<Text, IntWritable , Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int l = 0;
            for (IntWritable val : values) {
                l += val.get();
            }
            context.write(key, new IntWritable(l));
        }
    }




    @Override
    public int run(String[] strings) throws Exception {

        if(strings.length != 2){
            return 1;
        }

        String input = strings[0];
        String outPath = strings[1];


        Configuration configured = new Configuration();

        FileSystem fs = FileSystem.get(configured);
        Path path = new Path(outPath);

        System.out.println(fs.exists(path));

        boolean isOk = fs.deleteOnExit(path);
        if (isOk) {
            System.out.println("delete ok!");
        } else {
            System.out.println("delete failure");
        }
        fs.close();

        Job job = Job.getInstance(configured, "jobOne");


        job.setJarByClass(JobOne.class);
        job.setJobName("jobOne");

        job.setMapperClass(JobOneMapper.class);
        job.setReducerClass(JobOneReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // use the arguments to set the job input and output path
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception  {

        // 记录开始时间
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = new Date();
        int res = ToolRunner.run(new Configuration() , new JobOne(), args);

        // 输出任务耗时
        Date end = new Date();
        float time = (float) ((end.getTime() - start.getTime()) / 1000.0);
        System.out.println("任务开始：" + formatter.format(start));
        System.out.println("任务结束：" + formatter.format(end));
        System.out.println("任务耗时：" + String.valueOf(time) + " 秒");
        System.exit(res);
    }
}
