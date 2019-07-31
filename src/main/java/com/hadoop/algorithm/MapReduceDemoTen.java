package com.hadoop.algorithm;

import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created with IDEA by ChouFy on 2019/7/23.
 *
 * @author Zhoufy
 */
public class MapReduceDemoTen {


    public static class DemoTenStepOneMapper extends Mapper<LongWritable, Text, Text, Tuple> {

        @Override
        protected void map(LongWritable keys, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] content = line.split(",");
            String movie = content[1];
            String user = content[0];
            String rating = content[2];

            Schema schema = new Schema();
            schema.addField("user", String.class);
            schema.addField("rating", String.class);
            Tuple tuple = schema.instantiate();

            tuple.set("user", user);
            tuple.set("rating", rating);
            context.write(new Text(movie), tuple);
        }
    }

    public static class DemoTenStepOneReduce extends Reducer<Text, Tuple, Text, Tuple> {
        @Override
        protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            int numberOfRaters = 0;

            List<Tuple> list = new ArrayList<>();

            /*
                reduce方法的javadoc中已经说明了会出现的问题：

                The framework calls this method for each <key, (list of values)> pair in the grouped inputs.
                Output values must be of the same type as input values. Input keys must not be altered.
                The framework will reuse the key and value objects that are passed into the reduce,
                therefore the application should clone the objects they want to keep a copy of.

                也就是说虽然reduce方法会反复执行多次，但key和value相关的对象只有两个，reduce会反复重用这两个对象。
                所以如果要保存key或者value的结果，只能将其中的值取出另存或者重新clone一个对象
                （例如Text store = new Text(value) 或者 String a = value.toString()），
                而不能直接赋引用。因为引用从始至终都是指向同一个对象，你如果直接保存它们，
                那最后它们都指向最后一个输入记录。会影响最终计算结果而出错。
             */
            for (Tuple tuple2 : values) {
                numberOfRaters++;

                System.out.println("tuple2 : " + tuple2.toString());

                String user = tuple2.get("user").toString();
                String rating = tuple2.get("rating").toString();

                Schema schema = new Schema();
                schema.addField("user", String.class);
                schema.addField("rating", String.class);
                Tuple tuple = schema.instantiate();

                tuple.setSymbol("user", user);
                tuple.setSymbol("rating", rating);
                list.add(tuple);
            }

            System.out.println("numberOfRaters ：" + numberOfRaters);

            for (Tuple tuple2 : list) {
                String user = tuple2.getSymbol("user");
                String rating = tuple2.getSymbol("rating");
                System.out.println("第二次打印 :" + numberOfRaters);
                System.out.println("第二次打印：tuple2 : " + tuple2.toString());

                Schema schema = new Schema();
                schema.addField("movie", String.class);
                schema.addField("rating", String.class);
                schema.addField("numberOfRaters", String.class);
                Tuple tuple = schema.instantiate();

                tuple.setSymbol("movie", key.toString());
                tuple.setSymbol("rating", rating);
                tuple.setSymbol("numberOfRaters", String.valueOf(numberOfRaters));
                context.write(new Text(user), tuple);
            }
        }
    }

    public boolean stepOneRun(String inputPath, String outPath, Configuration conf, FileSystem fs) {
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(MapReduceDemoTen.class);
            job.setJobName("job1");

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Tuple.class);

            job.setMapperClass(DemoTenStepOneMapper.class);
            job.setReducerClass(DemoTenStepOneReduce.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Tuple.class);

            job.setNumReduceTasks(1);

            Path input = new Path(inputPath);
            Path output = new Path(outPath);

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


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(conf);

        if (args.length != 2) {
            throw new Exception("params length error");
        }
        String inputPath = args[0];
        String outPath = args[1];

        MapReduceDemoTen mapReduceDemoTen = new MapReduceDemoTen();
        mapReduceDemoTen.stepOneRun(inputPath, outPath, conf, fs);
    }


}
