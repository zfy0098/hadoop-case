package com.spark.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2019/7/14.
 *
 * @author Zhoufy
 */
public class Demo9 {

    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("");
        sparkConf.setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        JavaRDD<String> rdd = sparkContext.textFile("/Users/zhoufy/sparktest/demo9", 1);
        List<String> debug1 = rdd.collect();
        for (String line : debug1) {
            System.out.println("debug1 : " + line);
        }

        JavaPairRDD<Long, Tuple2<Long, Long>> pairs = rdd.flatMapToPair(record -> {
            String[] token = record.split("\t");
            long person = Long.parseLong(token[0]);
            String[] friendsTokenized = token[1].split(",");

            List<Long> friends = new ArrayList<>();
            List<Tuple2<Long, Tuple2<Long, Long>>> mapperOut = new ArrayList<>();

            for (String friendAsString : friendsTokenized) {
                long toUser = Long.parseLong(friendAsString);
                friends.add(toUser);

                Tuple2<Long, Long> directFriend = new Tuple2<>(toUser, -1L);
                mapperOut.add(new Tuple2<>(toUser, directFriend));
            }

            for (int i = 0; i < friends.size(); i++) {
                for (int j = i + 1; j < friends.size(); j++) {
                    Tuple2<Long, Long> p1 = new Tuple2<>(friends.get(j), person);
                    mapperOut.add(new Tuple2<>(friends.get(i), p1));

                    Tuple2<Long, Long> p2 = new Tuple2<>(friends.get(i), person);
                    mapperOut.add(new Tuple2<>(friends.get(j), p2));
                }
            }
            return mapperOut.iterator();
        });


        List<Tuple2<Long, Tuple2<Long, Long>>> debug2 = pairs.collect();
        for (Tuple2<Long, Tuple2<Long, Long>> t2 : debug2) {
            System.out.println("debug 2 :" + t2._1 + " , " + t2._2);
        }

        JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> grouped = pairs.groupByKey();

        List<Tuple2<Long, Iterable<Tuple2<Long, Long>>>> debug3 = grouped.collect();
        for (Tuple2<Long, Iterable<Tuple2<Long, Long>>> t2 : debug3) {
            System.out.println("debug 3 :" + t2._1 + " , " + t2._2);
        }

        JavaPairRDD<Long, String> recommendations = grouped.mapValues(values -> {
            final Map<Long, List<Long>> mutualFriends = new HashMap<>();
            for (Tuple2<Long, Long> t2 : values) {

                long toUser = t2._1;
                long mutualFriend = t2._2;
                boolean alreadyFriend = (-1 == mutualFriend);

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
                        List<Long> list1 = new ArrayList<>(Arrays.asList(mutualFriend));
                        mutualFriends.put(toUser, list1);
                    }
                }
            }

            return buildRecommendations(mutualFriends);
        });

        List<Tuple2<Long, String>> debug4 = recommendations.collect();

        for (Tuple2<Long, String> tuple2 : debug4){
            System.out.println("debug 4 :" + tuple2._1 + " , " + tuple2._2);

        }


        recommendations.saveAsTextFile("/Users/zhoufy/sparktest/demo9out/");
    }

    static String buildRecommendations(Map<Long, List<Long>> mutualFriends) {
        StringBuilder recommendations = new StringBuilder();
        for (Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            recommendations.append(entry.getKey());
            recommendations.append(" (");
            recommendations.append(entry.getValue().size());
            recommendations.append(": ");
            recommendations.append(entry.getValue());
            recommendations.append("),");
        }
        return recommendations.toString();
    }



}
