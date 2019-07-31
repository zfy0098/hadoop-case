package com.hadoop.algorithm;

import org.apache.calcite.util.Pair;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2019/7/13.
 *
 * @author Zhoufy
 */
public class MapReduceDemoNine {


    public static class DemoNineMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if(line.split("\t").length != 2){
                return ;
            }
            Text userid = new Text(line.split("\t")[0]);
            String[] friends = line.split("\t")[1].split(",");

            for (String friend : friends) {
//                Tuple2<Text, Text> tuple2 = new Tuple2<>(new Text(), new Text("-1"));
                context.write(userid, new Text(friend + "#### " + "-1"));
            }

            for (int i = 0; i < friends.length; i++) {
                for (int j = i + 1; j < friends.length; j++) {
//                    Tuple2<Text, Text> possibleFriend1 = new Tuple2<>(new Text(friends[j]), new Text(userid));
                    context.write(new Text(friends[i]), new Text(friends[j] + "####" + userid));

//                    Tuple2<Text, Text> possibleFriend2 = new Tuple2<>(new Text(friends[i]), new Text(userid));
                    context.write(new Text(friends[j]), new Text(friends[i] + "####" + userid));

                }
            }

        }
    }


    public static class DemoNineReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            Map<String, List<String>> mutualFriends = new HashMap<>();


            for (Text t2 : values) {
                String toUser = t2.toString().split("####")[0];
                String mutualFriend = t2.toString().split("####")[1];
                boolean alreadyFriend = "-1".equals(mutualFriend.toString());

                if (mutualFriends.containsKey(toUser)) {
                    if (alreadyFriend) {
                        mutualFriends.put(toUser, null);
                    } else if (mutualFriends.get(toUser) != null) {
                        mutualFriends.get(toUser).add(mutualFriend);
                    }
                } else {
                    if (alreadyFriend) {
                        mutualFriends.put(toUser, null);
                    } else {
                        mutualFriends.put(toUser, new ArrayList<>());
                    }
                }

            }


            String reducerOutput = buildOutput(mutualFriends);
            context.write(key, new Text(reducerOutput));


        }
    }


    public static String buildOutput(Map<String, List<String>> map) {
        String output = "";
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            String k = entry.getKey();
            List<String> v = entry.getValue();
            output += k + "(" + v.size() + " : " + v + ") , ";
        }
        return output;
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            throw new Exception("args length error");
        }

        String input = args[0];
        String out = args[1];


        Configuration configuration = new Configuration();

        Path path = new Path(out);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(path)) {
            fs.deleteOnExit(path);
        }
        fs.close();


        Job job = Job.getInstance(configuration);

        job.setJobName("demo nine");

        job.setJarByClass(MapReduceDemoNine.class);

        job.setMapperClass(MapReduceDemoNine.DemoNineMap.class);
        job.setReducerClass(MapReduceDemoNine.DemoNineReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(input));

        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);

    }

}
