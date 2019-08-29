package com.hadoop.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * 这里处理TF
 */
public class Job1 {



    public boolean run(Configuration conf, FileSystem fs) {
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(Job1.class);
            job.setJobName("job1");
            job.setMapperClass(Job1Mapper.class);
            job.setPartitionerClass(Job1Partition.class);
            job.setReducerClass(Job1Reducer.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            /*
                See : KeyValueTextInputFormat
                    一个InputFormat纯文本文件。
                    文件分为几行。换行或回车用于发出行尾信号。每行由分隔符字节分为键和值部分。
                    如果不存在这样的字节，则键将是整行，值将为空。
                    可以在属性名称mapreduce.input.keyvaluelinerecordreader.key.value.separator
                    下的配置文件中指定分隔符字节。默认值是制表符（'\t'）。

                 TextInputFormat，
                    它提供的 RecordReader 会将文本的行号作为 Key，这一行的文本作为 Value。这就是自定义 Mapper 的输入是 < LongWritable,Text> 的原因

             */
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setNumReduceTasks(4);

            Path input = new Path("/tfidf/weibo.txt");
            Path output = new Path("/tfidf/output1");

            if (!fs.exists(input)) {
                System.out.println("job1 输入文件不存在！");
                return false;
            }

            if (fs.exists(output)) {
                fs.delete(output, true);
            }

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            boolean res = job.waitForCompletion(true);
            if (res) {
                System.out.println("job1 执行完成！");
                return true;
            } else {
                System.out.println("job1 执行失败！");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}


class Job1Mapper extends Mapper<Text, Text, Text, IntWritable> {

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        String artical = value.toString();
        // 分词器
        StringReader sr =new StringReader(artical);
        IKSegmenter iks = new IKSegmenter(sr, true);
        Lexeme word = null;
        while ((word = iks.next()) != null) {
            // 获取分词结果的单词
            String w= word.getLexemeText();
            // 词语_id   1
            context.write(new Text(w+"_"+key), new IntWritable(1));
        }
        // |D|  count  1
        context.write(new Text("count"), new IntWritable(1));
    }
}

class Job1Partition extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if(key.equals(new Text("count")))
            return 3;
        else
            return super.getPartition(key, value, numReduceTasks-1);
    }
}

class Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}