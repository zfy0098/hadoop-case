package com.hadoop.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.utils.ParamsUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created with IDEA by ChouFy on 2019/8/6.
 *
 * @author Zhoufy
 */
public class CommonFriends {


    public static class CommonFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text REDUCER_KEY = new Text();
        private static final Text REDUCER_VALUE = new Text();

        static String getFriends(String[] tokens) {
            if (tokens.length == 2) {
                return "";
            }
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i < tokens.length; i++) {
                builder.append(tokens[i]);
                if (i < (tokens.length - 1)) {
                    builder.append(",");
                }
            }
            return builder.toString();
        }

        static String buildSortedKey(String person, String friend) {
            long p = Long.parseLong(person);
            long f = Long.parseLong(friend);
            if (p < f) {
                return person + "," + friend;
            } else {
                return friend + "," + person;
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // parse input, delimiter is a single space
            String[] tokens = StringUtils.split(value.toString(), " ");

            // create reducer value
            String friends = getFriends(tokens);
            REDUCER_VALUE.set(friends);

            String person = tokens[0];
            for (int i = 1; i < tokens.length; i++) {
                String friend = tokens[i];
                String reducerKeyAsString = buildSortedKey(person, friend);
                REDUCER_KEY.set(reducerKeyAsString);
                System.out.println("map key:" + REDUCER_KEY.toString() + " ,  value " + REDUCER_VALUE.toString());
                context.write(REDUCER_KEY, REDUCER_VALUE);
            }
        }
    }


    public static class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * The goal is to find common friends by intersecting all lists defined in values parameter.
         *
         * @param key    is a pair: <user_id_1><,><user_id_2>
         * @param values is a list of { <friend_1><,>...<,><friend_n> }
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<>(16);
            Iterator<Text> iterator = values.iterator();
            int numOfValues = 0;
            while (iterator.hasNext()) {
                String friends = iterator.next().toString();
                if (friends.equals("")) {
                    context.write(key, new Text("[]"));
                    return;
                }
                addFriends(map, friends);
                numOfValues++;
            }

            // now iterate the map to see how many have numOfValues
            List<String> commonFriends = new ArrayList<String>();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                System.out.println(entry.getKey() + "/" + entry.getValue());
                if (entry.getValue() == numOfValues) {
                    commonFriends.add(entry.getKey());
                }
            }

            // sen it to output
            context.write(key, new Text(commonFriends.toString()));
        }

        static void addFriends(Map<String, Integer> map, String friendsList) {
            String[] friends = StringUtils.split(friendsList, ",");
            for (String friend : friends) {
                Integer count = map.get(friend);
                if (count == null) {
                    map.put(friend, 1);
                } else {
                    map.put(friend, ++count);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {


        // vm options = -DHADOOP_USER_NAME=root
        args = new String[]{"hdfs://master:9000/friend/commonfriend", "hdfs://master:9000/friend/commonfriendout/"};
        Job job = ParamsUtils.pretreatment(args, 2);
        job.setJobName("commonfriend");
        job.setJarByClass(CommonFriends.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        job.setMapperClass(CommonFriendMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        job.waitForCompletion(true);
    }
}
