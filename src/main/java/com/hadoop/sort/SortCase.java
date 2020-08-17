package com.hadoop.sort;

import com.utils.ParamsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA by ChouFy on 2020/8/16.
 *
 * @author chouFy
 */
public class SortCase {


    public static class NumberModel implements WritableComparable<NumberModel> {

        private Integer number;

        public NumberModel(Integer number) {
            this.number = number;
        }

        public NumberModel() {
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(number);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.number = dataInput.readInt();
        }

        @Override
        public String toString() {
            return number + "";
        }

        @Override
        public int compareTo(NumberModel o) {
            if (number < o.number) {
                return -1;
            } else if (number > o.number) {
                return 1;
            }
            return 0;
        }


        public Integer getNumber() {
            return number;
        }

        public void setNumber(Integer number) {
            this.number = number;
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, NumberModel, NullWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Integer num = Integer.parseInt(line.trim());
            System.out.println(num);
            final NumberModel number = new NumberModel(num);
            context.write(number, NullWritable.get());
        }
    }


    public static class SortReducer extends Reducer<NumberModel, NullWritable, IntWritable, NumberModel> {

        int count = 1;

        @Override
        protected void reduce(NumberModel key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            for (NullWritable value : values) {
                context.write(new IntWritable(count), key);
                count++;
            }
        }
    }


    public static void main(String[] args) throws Exception {

        String input = "hdfs://master:9000/test1/";
        String out = "hdfs://master:9000/out";

        args = new String[]{input, out};
        // 读取hadoop配置
        Configuration conf = new Configuration();
        // 实例化一道作业
        Job job = ParamsUtils.pretreatment(args, 2);


        job.setJar(ParamsUtils.JAR_PATH);

        job.setJarByClass(SortCase.class);
        // Mapper类型
        job.setMapperClass(SortMapper.class);

        // 不再需要Combiner类型，因为Combiner的输出类型<Text, IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
        //job.setCombinerClass(Reduce.class);

        // Reducer类型
        job.setReducerClass(SortReducer.class);
        // 分区函数
//        job.setPartitionerClass(FirstPartitioner.class);
        // 分组函数
//        job.setGroupingComparatorClass(GroupingComparator.class);

        // map 输出Key的类型
        job.setMapOutputKeyClass(NumberModel.class);
        // map输出Value的类型
        job.setMapOutputValueClass(NullWritable.class);


        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat

        job.setOutputKeyClass(IntWritable.class);
        // rduce输出Value的类型
        job.setOutputValueClass(NumberModel.class);

        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
//        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
//        job.setOutputFormatClass(TextOutputFormat.class);

        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
