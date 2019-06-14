package com.hadoop.chapter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2019/3/5.
 *
 * @author Zhoufy
 */
public class Join {

    private static final String CUSTOMER_CACHE_URL = "hdfs://master:9000/join/a.txt";


    private static class A {
        String name;
        String number;

        public A(String name, String number) {
            this.name = name;
            this.number = number;
        }

        public A() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getNumber() {
            return number;
        }

        public void setNumber(String number) {
            this.number = number;
        }
    }


    private static class MapOutKey implements WritableComparable<MapOutKey> {

        private String aName;
        private String bName;

        public void set(String aName, String bName) {
            this.aName = aName;
            this.bName = bName;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(aName);
            out.writeUTF(bName);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            aName = in.readUTF();
            bName = in.readUTF();
        }

        @Override
        public int compareTo(MapOutKey o) {
            return aName.compareTo(o.aName);
        }

        @Override
        public String toString() {
            return aName + "\t" + bName;
        }

    }


    private static class JoinMapper extends Mapper<LongWritable, Text, MapOutKey, Text> {

        private static final Map<String, A> CUSTOMER_MAP = new HashMap<>();

        private final MapOutKey outputKey = new MapOutKey();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split("\t");
            if (cols.length != 2) {
                System.out.println("长度不等于2");
                return;
            }

            String name = cols[0];
            A customerBean = CUSTOMER_MAP.get(name);

            // 没有对应的customer信息可以连接
            if (customerBean == null) {
                System.out.println("customerBean is null");
                return;
            }

            StringBuffer sb = new StringBuffer();
            sb.append(customerBean.getName())
                    .append("\t")
                    .append(cols[1])
                    .append("\t")
                    .append(customerBean.getNumber());

            outputValue.set(sb.toString());

            outputKey.set(name, cols[0]);
            context.write(outputKey, outputValue);

        }

        @Override
        protected void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(URI.create(CUSTOMER_CACHE_URL), context.getConfiguration());
            FSDataInputStream fdis = fs.open(new Path(CUSTOMER_CACHE_URL));

            BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
            String line = null;
            String[] cols = null;

            // 格式：客户编号  姓名  地址  电话
            while ((line = reader.readLine()) != null) {
                cols = line.split("\t");
                if (cols.length != 2) {
                    System.out.println("length not dengyu 2");
                    continue;
                }
                A bean = new A(cols[0], cols[1]);
                CUSTOMER_MAP.put(bean.getName(), bean);
            }

        }
    }


    private static class JoinReducer extends Reducer<MapOutKey, Text, MapOutKey, Text> {
        @Override
        protected void reduce(MapOutKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 什么事都不用做，直接输出
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        String inPath;
        String outPath;

        if (args.length != 2) {
            return;
        }
        inPath = args[0];
        outPath = args[1];

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path(outPath))){
            fs.deleteOnExit(new Path(outPath));
        }
        fs.close();

        Job job = Job.getInstance(conf, "join");


        job.addCacheFile(URI.create(CUSTOMER_CACHE_URL));


        job.setJarByClass(Join.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputValueClass(MapOutKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(MapOutKey.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);

    }
}
