package com.hadoop.fof;

import com.utils.ParamsUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IDEA by ChouFy on 2019/8/5.
 *
 * @author Zhoufy
 */
public class StepOne {


    public static class StepOneMapper extends Mapper<LongWritable, Text, User, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 分隔字符串
            System.out.println(value.toString());
            String[] line = value.toString().split("\t");
            System.out.println("line.length:" + line.length);

            for (int i = 0; i < line.length; i++) {
                int relation = 1;
                if (i == 0) {
                    relation = 0;
                }
                for (int j = i + 1; j < line.length; j++) {
                    User user = new User(line[i], line[j], relation);
                    context.write(user, new IntWritable(relation));
                }
            }
        }
    }

    public static class StepOneReducer extends Reducer<User, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(User key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int relation = 0;
            for (IntWritable val : values) {
                if (val.get() == 0) {
                    break;
                }
                relation += val.get();
            }
            if (relation > 0) {
                context.write(new Text(key.format()), new IntWritable(relation));
            }
        }
    }

    public static class StepOneCombiner extends Reducer<User, IntWritable, User, IntWritable> {

        @Override
        protected void reduce(User key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int relation = 0;
            for (IntWritable val : values) {
                if (val.get() == 0) {
                    break;
                }
                relation += val.get();
            }
            if (relation > 0) {
                context.write(key, new IntWritable(relation));
            }
        }
    }


    public static class StepOneGroup extends WritableComparator {

        public StepOneGroup() {
            super(User.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            User user1 = (User) a;
            User user2 = (User) b;
            return user1.format().compareTo(user2.format());
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = ParamsUtils.pretreatment(args, 2);
        job.setJar("/Users/zhoufy/Documents/ideaProject/hadoop-case/out/artifacts/com_hadoop_jar/com.hadoop.jar");

        job.setJarByClass(StepOne.class);
        job.setJobName("jobOne");

        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(StepOneReducer.class);
        job.setNumReduceTasks(1);


        /*
         * 进入同一个reduce的key是按照顺序排好的，该类使得：
         * 如果连续（注意，一定连续）的两条或多条记录满足同组（即compare方法返回0）的条件，
         * 即使key不相同，他们的value也会进入同一个values,执行一个reduce方法。
         * 相反，如果原来key相同，但是并不满足同组的条件，他们的value也不会进入一个valeus。
         * 最后返回的key是：满足这些条件的一组key中第一组。
         */
        job.setGroupingComparatorClass(StepOneGroup.class);

        job.setMapOutputKeyClass(User.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

    }
}
