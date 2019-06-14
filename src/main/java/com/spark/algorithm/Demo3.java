package com.spark.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Demo3 implements Serializable {


    private Demo3() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("demo3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD javaRDD = sc.textFile("/Users/zhoufy/sparktest/demo3");

        JavaPairRDD<String, Integer> pair = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] tokens = s.split(",");
                System.out.println(tokens[0] + ",    " + tokens[1]);
                return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1].trim()));
            }
        });


        List<Tuple2<String, Integer>> debug1 = pair.collect();
        for (Tuple2<String, Integer> t2 : debug1) {
            System.out.println("key=" + t2._1 + "\t value= " + t2._2);
        }


        JavaRDD<SortedMap<Integer, String>> partitions = pair.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> iter) throws Exception {

                SortedMap<Integer, String> top10 = new TreeMap<>();
                while (iter.hasNext()) {
                    Tuple2<String, Integer> tuple = iter.next();
                    top10.put(tuple._2, tuple._1);

                    if (top10.size() > 10) {
                        top10.remove(top10.firstKey());
                    }
                }
                return Collections.singletonList(top10).iterator();
            }
        });

        SortedMap<Integer, String> finalTop10 = new TreeMap<>();

        List<SortedMap<Integer, String>> alltop10 = partitions.collect();
        for (SortedMap<Integer, String> localtop10 : alltop10) {

            for (Map.Entry<Integer, String> entry : localtop10.entrySet()) {
                finalTop10.put(entry.getKey(), entry.getValue());

                if (finalTop10.size() > 10) {
                    finalTop10.remove(finalTop10.firstKey());
                }
            }
        }

        System.out.println("========================");

        for (Map.Entry<Integer, String> map : finalTop10.entrySet()) {
            System.out.println(map.getKey() + ", " + map.getValue());
        }

    }

    public static void main(String[] args) {
        new Demo3();
    }

}
